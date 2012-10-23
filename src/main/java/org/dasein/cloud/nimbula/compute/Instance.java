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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VmStatistics;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.nimbula.NimbulaDirector;
import org.dasein.cloud.nimbula.NimbulaMethod;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Instance implements VirtualMachineSupport {
    static private final Logger logger = NimbulaDirector.getLogger(Instance.class);
    
    static public final String INSTANCE   = "instance";
    static public final String LAUNCHPLAN = "launchplan";
    static public final String SHAPE      = "shape";
    
    private NimbulaDirector cloud;
    
    Instance(@Nonnull NimbulaDirector cloud) { this.cloud = cloud; }
    
    @Override
    public void unpause(@Nonnull String vmId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Pause/unpause is not supported");
    }

    @Override
    public @Nonnull VirtualMachine clone(@Nonnull String vmId, @Nonnull String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String ... firewallIds) throws InternalException, CloudException {
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
    public @Nonnull String getConsoleOutput(@Nonnull String vmId) throws InternalException, CloudException {
        return "";
    }

    @Override
    public int getMaximumVirtualMachineCount() throws CloudException, InternalException {
        return -2;
    }

    @Override 
    public @Nullable VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(cloud, SHAPE);
        int code = method.get("/" + productId);
            
        if( code == 404 || code == 401 ) {
            return null;
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
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "instance";
    }

    @Override
    public @Nonnull VmStatistics getVMStatistics(@Nonnull String vmId, long from, long to) throws InternalException, CloudException {
        return new VmStatistics();
    }

    @Override
    public @Nonnull Iterable<VmStatistics> getVMStatisticsForPeriod(@Nonnull String vmId, long from, long to) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Requirement identifyPasswordRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyRootVolumeRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyVlanRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public boolean isAPITerminationPreventable() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isBasicAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isExtendedAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nullable VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
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
        int code = method.get(vmId);
            
        if( code == 404 || code == 401 ) {
            return null;
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

    @Override
    public boolean isUserDataSupported() throws CloudException, InternalException {
        return false;
    }

    static private class LaunchInfo {
        public int entry;
        public String imageList;
    }
    
    private LaunchInfo getLaunchInfo(String imageId) throws CloudException, InternalException {
        String[] idInfo = cloud.parseId(imageId);
        
        NimbulaMethod method = new NimbulaMethod(cloud, Image.IMAGELIST);
        
        method.get("/" + idInfo[0] + "/" + idInfo[1] + "/");
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
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nonnull String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String ... firewalls) throws InternalException, CloudException {
        return launch(fromMachineImageId, product, dataCenterId, name, description, withKeypairId, inVlanId, withAnalytics, asSandbox, firewalls, new Tag[0]);
    }
    
    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nonnull String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String[] firewalls, @Nullable Tag ... tags) throws InternalException, CloudException {
        VMLaunchOptions options;

        if( inVlanId == null ) {
            options = VMLaunchOptions.getInstance(product.getProviderProductId(), fromMachineImageId, name, description).inDataCenter(dataCenterId);
        }
        else {
            options = VMLaunchOptions.getInstance(product.getProviderProductId(), fromMachineImageId, name, description).inVlan(null, dataCenterId, inVlanId);
        }
        if( withKeypairId != null ) {
            options = options.withBoostrapKey(withKeypairId);
        }
        if( tags != null ) {
            for( Tag t : tags ) {
                options = options.withMetaData(t.getKey(), t.getValue());
            }
        }
        if( firewalls != null ) {
            options = options.behindFirewalls(firewalls);
        }
        return launch(options);
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions options) throws CloudException, InternalException {
        HashMap<String,Object> state = new HashMap<String,Object>();
        LaunchInfo launch = getLaunchInfo(options.getMachineImageId());
        state.put("relationships", new ArrayList<String>());
        
        ArrayList<Map<String,Object>> targets = new ArrayList<Map<String,Object>>();
        HashMap<String,Object> plan = new HashMap<String,Object>();
        
        plan.put("label", options.getFriendlyName());
        plan.put("shape", options.getStandardProductId());
        plan.put("imagelist", launch.imageList);
        plan.put("entry", launch.entry);
        if( options.getFirewallIds().length > 0 ) {
            ArrayList<String> ids = new ArrayList<String>();

            Collections.addAll(ids, options.getFirewallIds());
            plan.put("seclists", ids);
        }
        targets.add(plan);
        
        state.put("instances", targets);
        
        NimbulaMethod method = new NimbulaMethod(cloud, LAUNCHPLAN);
        
        method.post(state);

        try {
            JSONArray instances = method.getResponseBody().getJSONArray("instances");
            
            if( instances.length() < 1 ) {
                throw new CloudException("Cloud failed to launch any instances without comment.");
            }
            VirtualMachine vm = toVirtualMachine(instances.getJSONObject(0));

            if( vm == null ) {
                throw new CloudException("No virtual machine was created, but no error was specified");
            }
            return vm;
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
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        try {
            JSONObject ob = getInstance(vmId);
            
            if( ob == null || !ob.has("name") ) {
                throw new CloudException("No such instance: " + vmId);
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
    public @Nonnull Iterable<VirtualMachineProduct> listProducts(@Nonnull Architecture architecture) throws InternalException, CloudException {
        if( architecture.equals(Architecture.I32) ) {
            return Collections.emptyList();
        }
        NimbulaMethod method = new NimbulaMethod(cloud, SHAPE);
        
        method.list();
        try {
            ArrayList<VirtualMachineProduct> products = new ArrayList<VirtualMachineProduct>();
            JSONArray array = method.getResponseBody().getJSONArray("result");
            
            for( int i=0; i<array.length(); i++ ) {
                products.add(toProduct(array.getJSONObject(i)));
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
    public Iterable<Architecture> listSupportedArchitectures() throws InternalException, CloudException {
        return Collections.singletonList(Architecture.I64);
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(cloud, INSTANCE);
        
        method.list();

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
    public void pause(@Nonnull String vmId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Pause/unpause not supported");
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Reboots not supported");
    }

    @Override
    public void resume(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Suspend/resume not supported");
    }

    @Override
    public void start(@Nonnull String vmId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Start/stop not supported");
    }

    @Override
    public void stop(@Nonnull String vmId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Start/stop not supported");
    }

    @Override 
    public boolean supportsAnalytics() {
        return false;
    }

    @Override
    public boolean supportsPauseUnpause(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public boolean supportsSuspendResume(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public void suspend(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Suspend/resume not supported");
    }

    @Override
    public void terminate(@Nonnull String vmId) throws InternalException, CloudException {
        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20L);
        NimbulaMethod method = new NimbulaMethod(cloud, INSTANCE);
        VirtualMachine vm = getVirtualMachine(vmId);

        while( timeout > System.currentTimeMillis() ) {
            if( vm == null || !VmState.PENDING.equals(vm.getCurrentState()) ) {
                break;
            }
            try { Thread.sleep(5000L); }
            catch( InterruptedException ignore ) { }
            vm = getVirtualMachine(vmId);
        }
        method.delete(vmId);
        while( timeout > System.currentTimeMillis() ) {
            if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) ) {
                return;
            }
            try { Thread.sleep(5000L); }
            catch( InterruptedException ignore ) { }
            vm = getVirtualMachine(vmId);
        }
        throw new CloudException("The system timed out waiting for the virtual machine to terminate");
    }

    private @Nonnull VirtualMachineProduct toProduct(@Nonnull JSONObject ob) throws JSONException {
        VirtualMachineProduct product = new VirtualMachineProduct();
        
        product.setProviderProductId(ob.getString("name"));
        product.setRamSize(new Storage<Megabyte>(ob.getInt("ram"), Storage.MEGABYTE));
        if( ob.has("cpus") ) {
            product.setCpuCount((int)ob.getDouble("cpus"));
        }
        if( product.getCpuCount() < 1 ) {
            product.setCpuCount(1);
        }
        product.setDescription(ob.getString("name"));
        product.setName(ob.getString("name"));
        product.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
        if( product.getRamSize().intValue() < 256 ) {
            product.setRamSize(new Storage<Megabyte>(256, Storage.MEGABYTE));
        }
        return product;
    }
    
    private @Nonnull VmState toState(@Nonnull String value) {
        if( value.equalsIgnoreCase("running") ) {
            return VmState.RUNNING;
        }
        else if( value.equalsIgnoreCase("initializing") || value.equalsIgnoreCase("starting") || value.equalsIgnoreCase("queued") ) {
            return VmState.PENDING;
        }
        else if( value.equalsIgnoreCase("terminating") ) {
            return VmState.STOPPING;
        }
        logger.warn("DEBUG: Unknown Nimbula VM state: " + value);
        return VmState.PENDING;
    }
    
    private @Nullable VirtualMachine toVirtualMachine(@Nullable JSONObject ob) throws JSONException, InternalException, CloudException {
        if( ob == null ) {
            return null;
        }

        ProviderContext ctx = cloud.getContext();

        if( ctx == null ) {
            throw new CloudException("No context set for this request");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request");
        }
        String desc = ob.getString("name");
        VirtualMachine vm = new VirtualMachine();
        String[] idInfo = cloud.parseId(ob.getString("name"));

        vm.setProviderRegionId(regionId);
        vm.setProviderDataCenterId(regionId);
        vm.setProductId(ob.getString("shape"));
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
