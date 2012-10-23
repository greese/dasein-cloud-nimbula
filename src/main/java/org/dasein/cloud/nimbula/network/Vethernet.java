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

package org.dasein.cloud.nimbula.network;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import org.apache.commons.httpclient.HttpException;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.NetworkInterface;
import org.dasein.cloud.network.Subnet;
import org.dasein.cloud.network.VLANSupport;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.nimbula.NimbulaDirector;
import org.dasein.cloud.nimbula.NimbulaMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Vethernet implements VLANSupport {
    static private final Logger logger = NimbulaDirector.getLogger(VLANSupport.class);
    
    static public final String VDHCPD = "vdhcpd";
    
    private NimbulaDirector cloud;
    
    Vethernet(@Nonnull NimbulaDirector cloud) {
        this.cloud = cloud;
    }
    
    @Override
    public boolean allowsNewVlanCreation() throws CloudException, InternalException {
        return cloud.getContext().getAccountNumber().equals("root");
    }

    private int findId() throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, "vethernet");
        
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
            JSONArray array = method.getResponseBody().getJSONArray("result");
            boolean found;
            
            int id = 1;
            
            do {
                found = false;
                for( int i=0; i<array.length(); i++ ) {
                    JSONObject ob = array.getJSONObject(i);
                    int current = ob.getInt("id");
                    
                    if( current == id ) {
                        id++;
                        found = true;
                        break;
                    }
                } 
            } while( found );
            return id;
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e);
        }  
    }
    
    private JSONObject findVdhcpd(String vlanId) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, "vdhcpd");
        
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
            JSONArray array = method.getResponseBody().getJSONArray("result");
            
            for( int i=0; i<array.length(); i++ ) {
                JSONObject ob = array.getJSONObject(i);
                String id = ob.getString("vethernet");
                
                if( id != null && id.equals(vlanId) ) {
                    return ob;
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
    
    /*
    private String[] findFree() throws CloudException, InternalException {
        String[] network = new String[4];
        
        NimbulaMethod method = new NimbulaMethod(cloud, "vdhcpd");
        
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
            JSONArray array = method.getResponseBody().getJSONArray("result");
            int b = 0, c = 0;
            
            while( b < 255 ) {
                while( c < 255 ) {
                    String ip = "10." + b + "." + c;
                    boolean found = false;

                    for( int i=0; i<array.length(); i++ ) {
                        JSONObject ob = array.getJSONObject(i);
                        String startIp = ob.getString("iprange_start");

                        if( startIp.startsWith(ip) ) {
                            found = true;
                            break;
                        }
                    }
                    if( !found ) {
                        network[0] = ip + ".0/24";
                        network[1] = ip + ".254";
                        network[2] = ip + ".253";
                        network[3] = ip + ".252";
                        return network;
                    }
                    c++;
                }
                c = 0;
                b++;
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
    */
    
    @Override
    public int getMaxVlanCount() throws CloudException, InternalException {
        return 0;
    }

    @Override
    public @Nullable VLAN getVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, "vethernet");
     
        try {
            int code = method.get(vlanId);
            
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
            VLAN vlan = toVlan(method.getResponseBody());

            setNetwork(vlan);
            return vlan;
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
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    @Override
    public Iterable<VLAN> listVlans() throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, "vethernet");
        
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
            ArrayList<VLAN> vlans = new ArrayList<VLAN>();
            JSONArray array = method.getResponseBody().getJSONArray("result");
            
            for( int i=0; i<array.length(); i++ ) {
                VLAN vlan = toVlan(array.getJSONObject(i));
                
                if( vlan != null ) {
                    setNetwork(vlan);
                    vlans.add(vlan);
                }
            }
            return vlans;
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
    public Subnet createSubnet(String cidr, String inProviderVlanId, String name, String description) throws CloudException, InternalException {
        throw new OperationNotSupportedException();
    }
    
    
    @Override
    public VLAN createVlan(String cidr, String name, String description, String domainName, String[] dnsServers, String[] ntpServers) throws CloudException, InternalException {
        if( !cloud.getContext().getAccountNumber().equals("root") ) {
            throw new OperationNotSupportedException("VLAN creation is not yet supported");
        }
        int id = findId();
        
        int[] parts = new int[4];
        int idx = cidr.indexOf('/');
        int mask = 32;
        
        if( idx > -1 ) {
            mask = Integer.parseInt(cidr.substring(idx+1));
            cidr = cidr.substring(0, idx); 
        }
        
        String[] dotted = cidr.split("\\.");
        if( dotted.length != 4 ) {
            throw new CloudException("Invalid IP address: " + cidr);
        }
        int i = 0;
        
        for( String dot : dotted ) {
            try {
                parts[i++] = Integer.parseInt(dot);
            }
            catch( NumberFormatException e ) {
                throw new CloudException("Invalid IP address: " + cidr);
            }
        }
        HashMap<String,Object> state = new HashMap<String,Object>();
        
        state.put("id", id);
        state.put("description", description);
        state.put("type", "vlan");
        state.put("uri", null);
        try {
            state.put("name", "/" + cloud.getContext().getAccountNumber() + "/" + new String(cloud.getContext().getAccessPublic(), "utf-8") + "/vnet" + id);
        }
        catch( UnsupportedEncodingException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Failed UTF-8 encoding: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e);
        }
        NimbulaMethod method = new NimbulaMethod(cloud, "vethernet");
        
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
        VLAN vlan;
        
        try {
            vlan = toVlan(method.getResponseBody());
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        state.clear();

        String ipStop;
        if( mask == 32 || mask == 31) {
            ipStop = cidr;
        }
        else {
            if( mask >= 24 ) {
                int count = ((int)Math.pow(2,(32-mask))-1);

                if( count > 4 ) {
                    count = count - 4;
                }
                parts[3] = parts[3] + count;
            }
            else if( mask >= 16 ) {
                int count = ((int)Math.pow(2,(24-mask))-1);

                if( count > 4 ) {
                    count = count - 4;
                }
                parts[2] = parts[2] + count;
            }
            else if( mask >= 8 ) {
                int count = ((int)Math.pow(2,(16-mask))-1);

                if( count > 4 ) {
                    count = count - 4;
                }                
                parts[1] = parts[1] + count;
            }
            else {
                int count = ((int)Math.pow(2,(8-mask))-1);

                if( count > 4 ) {
                    count = count - 4;
                }                
                parts[0] = parts[0] + count;                
            }
            ipStop = parts[0] + "." + parts[1] + "." + parts[2] + "." + parts[3];
        }
        state.put("iprange_mask", mask);
        state.put("dns_server", dnsServers[0]);
        state.put("dns_server_standby", dnsServers[1]);
        state.put("iprange_start", cidr);
        state.put("iprange_stop", ipStop);
        state.put("uri", null);
        try {
            state.put("vethernet", "/" + cloud.getContext().getAccountNumber() + "/" + new String(cloud.getContext().getAccessPublic(), "utf-8") + "/vnet" + id);
            state.put("name", "/" + cloud.getContext().getAccountNumber() + "/" + new String(cloud.getContext().getAccessPublic(), "utf-8") + "/vdhcpd" + id);
        }
        catch( UnsupportedEncodingException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Encoding error: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e); 
        }
        int slash = cidr.indexOf('/');
        state.put("iprouter", cidr.subSequence(0,slash));
        method = new NimbulaMethod(cloud, VDHCPD);
        
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
        return vlan;
    }

    @Override
    public void removeVlan(String vlanId) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, "vethernet");
        
        try {
            method.delete(vlanId);
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
        JSONObject ob = findVdhcpd(vlanId);
        
        if( ob != null ) {
            method = new NimbulaMethod(cloud, "vdhcpd");
            try {
                method.delete(ob.getString("name"));
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
            catch( JSONException e ) {
                if( logger.isDebugEnabled() ) {
                    logger.error("Error parsing JSON: " + e.getMessage());
                    e.printStackTrace();
                }
                throw new InternalException(e);
            }             
        }
    }
    
    private void setNetwork(VLAN vlan) throws CloudException, InternalException {
        try {
            JSONObject ob = findVdhcpd(vlan.getProviderVlanId());
            
            if( ob != null ) {
                vlan.setDnsServers(new String[] { ob.getString("dns_server"), ob.getString("dns_server_standby") });
                vlan.setCidr(ob.getString("iprange_start") + "/" + ob.getString("iprange_mask"));
                vlan.setGateway(ob.getString("iprouter"));
                return;                
            }
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
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private VLAN toVlan(JSONObject ob) throws JSONException {
        String name = ob.getString("name");
        VLAN vlan = new VLAN();
        String[] idInfo = cloud.parseId(name);

        vlan.setDescription(ob.getString("description"));
        vlan.setName(idInfo[2]);
        vlan.setProviderVlanId(name);
        vlan.setProviderOwnerId(idInfo[0]);
        vlan.setProviderRegionId(cloud.getContext().getRegionId());
        vlan.setProviderDataCenterId(vlan.getProviderRegionId() + "-a");
        return vlan;
    }

    @Override
    public boolean allowsNewSubnetCreation() throws CloudException, InternalException {
        return false;
    }

    @Override
    public String getProviderTermForNetworkInterface(Locale locale) {
        return "network interface";
    }

    @Override
    public String getProviderTermForSubnet(Locale locale) {
        return "subnet";
    }

    @Override
    public String getProviderTermForVlan(Locale locale) {
        return "vethernet";
    }

    @Override
    public Subnet getSubnet(String subnetId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public boolean isSubnetDataCenterConstrained() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isVlanDataCenterConstrained() throws CloudException, InternalException {
        return true;
    }

    @Override
    public Iterable<NetworkInterface> listNetworkInterfaces(String forVmId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public Iterable<Subnet> listSubnets(String inVlanId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public void removeSubnet(String providerSubnetId) throws CloudException, InternalException {
        throw new OperationNotSupportedException();
    }

    @Override
    public boolean supportsVlansWithSubnets() throws CloudException, InternalException {
        return false;
    }
}
