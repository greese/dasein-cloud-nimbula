/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.nimbula.compute;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.httpclient.HttpException;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VmStatistics;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.nimbula.NimbulaDirector;
import org.dasein.cloud.nimbula.NimbulaMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;

public class Instance implements VirtualMachineSupport {
    static private final Logger logger = NimbulaDirector.getLogger(Instance.class);
    
    static public final String INSTANCE   = "instance";
    static public final String LAUNCHPLAN = "launchplan";
    static public final String SHAPE      = "shape";
    
    private NimbulaDirector cloud;
    
    Instance(@Nonnull NimbulaDirector cloud) { this.cloud = cloud; }
    
    @Override
    public void boot(String vmId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Nimbula does not yet support VM pausing/booting.");
    }

    @Override
    public VirtualMachine clone(String vmId, String intoDcId, String name, String description, boolean powerOn, String ... firewallIds) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Cloning is not currently supported.");
    }

    @Override
    public void disableAnalytics(String vmId) throws InternalException, CloudException {
        // NO-OP
    }

    @Override
    public void enableAnalytics(String vmId) throws InternalException, CloudException {
        // NO-OP
    }

    @Override
    public String getConsoleOutput(String vmId) throws InternalException, CloudException {
        return "";
    }

    @Override 
    public VirtualMachineProduct getProduct(String productId) throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(cloud, SHAPE);
        
        try {
            int code = method.get("/" + productId);
            
            if( code == 404 || code == 401 ) {
                return null;
            }
        }
        catch( HttpException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        catch( IOException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        try {
            return toProduct(method.getResponseBody());
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e);
        }  
    }
    
    @Override
    public String getProviderTermForServer(Locale locale) {
        return "instance";
    }

    @Override
    public VmStatistics getVMStatistics(String vmId, long from, long to) throws InternalException, CloudException {
        return new VmStatistics();
    }

    @Override
    public Iterable<VmStatistics> getVMStatisticsForPeriod(String vmId, long from, long to) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public VirtualMachine getVirtualMachine(String vmId) throws InternalException, CloudException {
        try {
            return toVirtualMachine(getInstance(vmId));
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e);
        } 
    }
     
    private JSONObject getInstance(String vmId) throws InternalException, CloudException, JSONException {
        NimbulaMethod method = new NimbulaMethod(cloud, INSTANCE);
        
        try {
            int code = method.get(vmId);
            
            if( code == 404 || code == 401 ) {
                return null;
            }
        }
        catch( HttpException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        catch( IOException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        return method.getResponseBody();
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, INSTANCE);
        
        try {
            method.list();
            return true;
        }
        catch( CloudException e ) {
            String msg = e.getMessage();
            
            if( msg.startsWith("401:") || msg.startsWith("403:") ) {
                return false;
            }
            throw e;
        }
        catch( InternalException e ) {
            String msg = e.getMessage();
            
            if( msg.startsWith("401:") || msg.startsWith("403:") ) {
                return false;
            }
            throw e;
        }
        catch( RuntimeException e ) {
            logger.error("Error testing subscription: " + e.getMessage());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new InternalException(e);
        }
        catch( Exception e ) {
            logger.error("Error testing subscription: " + e.getMessage());
            if( logger.isDebugEnabled() ) {
                e.printStackTrace();
            }
            throw new InternalException(e);            
        }
    }

    static private class LaunchInfo {
        public int entry;
        public String imageList;
    }
    
    private LaunchInfo getLaunchInfo(String imageId) throws CloudException, InternalException {
        String[] idInfo = cloud.parseId(imageId);
        
        NimbulaMethod method = new NimbulaMethod(cloud, Image.IMAGELIST);
        
        try {
            method.get("/" + idInfo[0] + "/" + idInfo[1] + "/");
        }
        catch( HttpException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        catch( IOException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        try {
            JSONArray array = method.getResponseBody().getJSONArray("result");

            for( int i=0; i<array.length(); i++ ) {
                JSONObject ob = array.getJSONObject(i);
                JSONArray entries = ob.getJSONArray("entries");
                String imageList = ob.getString("name");

                for( int j=0; j<entries.length(); j++ ) {
                    JSONObject entry = entries.getJSONObject(j);
                    JSONArray images = entry.getJSONArray("machineimages");
                    
                    for( int k=0; k<images.length(); k++ ) {
                        String id = images.getString(k);
                    
                        if( id.equals(imageId) ) {
                            LaunchInfo launchInfo = new LaunchInfo();
                            
                            launchInfo.entry = k+1;
                            launchInfo.imageList = imageList;
                            return launchInfo;
                        }
                    }
                }
            }
            return null;
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e);
        }
    }
    
    @Override
    public VirtualMachine launch(String fromMachineImageId, VirtualMachineProduct product, String dataCenterId, String name, String description, String withKeypairId, String inVlanId, boolean withAnalytics, boolean asSandbox, String ... firewalls) throws InternalException, CloudException {
        return launch(fromMachineImageId, product, dataCenterId, name, description, withKeypairId, inVlanId, withAnalytics, asSandbox, firewalls, new Tag[0]);
    }
    
    @Override
    public VirtualMachine launch(String fromMachineImageId, VirtualMachineProduct product, String dataCenterId, String name, String description, String withKeypairId, String inVlanId, boolean withAnalytics, boolean asSandbox, String[] firewalls, Tag ... tags) throws InternalException, CloudException {
        HashMap<String,Object> state = new HashMap<String,Object>();
        LaunchInfo launch = getLaunchInfo(fromMachineImageId);
        state.put("relationships", new ArrayList<String>());
        
        ArrayList<Map<String,Object>> targets = new ArrayList<Map<String,Object>>();
        HashMap<String,Object> plan = new HashMap<String,Object>();
        
        plan.put("label", name);
        plan.put("shape", product.getProductId());
        plan.put("imagelist", launch.imageList);
        plan.put("entry", launch.entry);
        if( firewalls != null && firewalls.length > 0 ) {
            ArrayList<String> ids = new ArrayList<String>();
            
            for( String fwid : firewalls ) {
                ids.add(fwid);
            }
            plan.put("seclists", ids);
        }
        targets.add(plan);
        
        state.put("instances", targets);
        
        NimbulaMethod method = new NimbulaMethod(cloud, LAUNCHPLAN);
        
        try {
            method.post(state);
        }
        catch( HttpException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        catch( IOException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        try {
            JSONArray instances = method.getResponseBody().getJSONArray("instances");
            
            if( instances.length() < 1 ) {
                throw new CloudException("Cloud failed to launch any instances without comment.");
            }
            return toVirtualMachine(instances.getJSONObject(0));
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
    }

    @Override
    public Iterable<String> listFirewalls(String vmId) throws InternalException, CloudException {
        try {
            JSONObject ob = getInstance(vmId);
            
            if( ob == null || !ob.has("name") ) {
                return null;
            }
            if( !ob.has("seclists") ) {
                return Collections.emptyList();
            }
            ArrayList<String> ids = new ArrayList<String>();
            JSONArray arr = ob.getJSONArray("seclists");
            
            for( int i=0; i<arr.length(); i++ ) {
                ids.add(arr.getString(i));
            }
            return ids;
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
    }

    @Override
    public Iterable<VirtualMachineProduct> listProducts(Architecture architecture) throws InternalException, CloudException {
        if( architecture.equals(Architecture.I32) ) {
            return Collections.emptyList();
        }
        NimbulaMethod method = new NimbulaMethod(cloud, SHAPE);
        
        try {
            method.list();
        }
        catch( HttpException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        catch( IOException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        try {
            ArrayList<VirtualMachineProduct> products = new ArrayList<VirtualMachineProduct>();
            JSONArray array = method.getResponseBody().getJSONArray("result");
            
            for( int i=0; i<array.length(); i++ ) {
                VirtualMachineProduct product = toProduct(array.getJSONObject(i));

                if( product != null ) {
                    products.add(product);
                }
            }
            return products;
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e);
        }
    }

    @Override
    public Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(cloud, INSTANCE);
        
        try {
            method.list();
        }
        catch( HttpException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        catch( IOException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        try {
            ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();
            JSONArray array = method.getResponseBody().getJSONArray("result");
            
            for( int i=0; i<array.length(); i++ ) {
                VirtualMachine vm = toVirtualMachine(array.getJSONObject(i));
                
                if( vm != null ) {
                    vms.add(vm);
                }
            }
            return vms;
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e);
        } 
    }

    @Override
    public void pause(String vmId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Nimbula does not yet support server pausing.");
    }

    @Override
    public void reboot(String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Nimbula does not yet support server reboots");
    }

    @Override 
    public boolean supportsAnalytics() {
        return false;
    }
    
    @Override
    public void terminate(String vmId) throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(cloud, INSTANCE);
        VirtualMachine vm = getVirtualMachine(vmId);
        
        while( vm != null && VmState.PENDING.equals(vm.getCurrentState()) ) {
            try { Thread.sleep(5000L); }
            catch( InterruptedException ignore ) { }
            vm = getVirtualMachine(vmId);
        }
        try {
            method.delete(vmId);
        }
        catch( HttpException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        catch( IOException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error in API call: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
    }
    
    private VirtualMachineProduct toProduct(JSONObject ob) throws JSONException { 
        VirtualMachineProduct product = new VirtualMachineProduct();
        
        product.setProductId(ob.getString("name"));
        product.setRamInMb(ob.getInt("ram"));
        product.setCpuCount((int)ob.getDouble("cpus"));
        product.setDescription(ob.getString("name"));
        product.setName(ob.getString("name"));
        product.setDiskSizeInGb(1);
        if( product.getRamInMb() < 256 ) {
            product.setRamInMb(256);
        }
        return product;
    }
    
    private VmState toState(String value) {
        if( value.equalsIgnoreCase("running") ) {
            return VmState.RUNNING;
        }
        else if( value.equalsIgnoreCase("initializing") || value.equalsIgnoreCase("starting") || value.equalsIgnoreCase("queued") ) {
            return VmState.PENDING;
        }
        else if( value.equalsIgnoreCase("terminating") ) {
            return VmState.STOPPING;
        }
        throw new RuntimeException("Unknown state: " + value);
    }
    
    private VirtualMachine toVirtualMachine(JSONObject ob) throws JSONException, InternalException, CloudException {
        if( ob == null ) {
            return null;
        }
        String desc = ob.getString("name");
        VirtualMachine vm = new VirtualMachine();
        String[] idInfo = cloud.parseId(ob.getString("name"));

        vm.setProviderRegionId(cloud.getContext().getRegionId());
        vm.setProviderDataCenterId(cloud.getContext().getRegionId());
        vm.setProduct(getProduct(ob.getString("shape")));
        vm.setClonable(false);
        vm.setImagable(true);
        vm.setPausable(false);
        vm.setPersistent(false);
        vm.setArchitecture(Architecture.I64);
        vm.setRebootable(false);
        vm.setCurrentState(toState(ob.getString("state")));
        vm.setDescription(ob.getString("label") + " [" + desc + "]");
        vm.setLastPauseTimestamp(-1L);
        vm.setName(ob.getString("label"));
        vm.setPlatform(Platform.UNKNOWN);
        vm.setPrivateDnsAddress(null);
        try {
            String ip = ob.getString("ip");
            
            if( ip != null ) {
                vm.setPrivateIpAddresses(new String[] { ip });                
            }
        }
        catch( JSONException ignore ) {
            // ignore
        }
        try {
            String startTime = ob.getString("start_time");

            if( startTime != null ) {
                try {
                    long ts = cloud.parseTimestamp(startTime);
             
                    vm.setCreationTimestamp(ts);
                    vm.setLastBootTimestamp(ts);
                }
                catch( ParseException e ) {
                    logger.warn("Error parsing time: " + startTime);
                }                
            }
        }
        catch( JSONException ignore ) {
            // ignore
        }
        vm.setProviderAssignedIpAddressId(null);
        String imagelist = ob.getString("imagelist");
        String entry = ob.getString("entry");
        vm.setProviderMachineImageId(cloud.getComputeServices().getImageSupport().getMachineImageId(imagelist, Integer.parseInt(entry)));
        vm.setProviderOwnerId(idInfo[0]);
        vm.setProviderVirtualMachineId(ob.getString("name"));
        vm.setPublicDnsAddress(null);
        vm.setPublicIpAddresses(new String[0]);
        vm.setRootPassword(null);
        vm.setRootUser(null);
        vm.setTerminationTimestamp(-1L);
        return vm;
    }
}
