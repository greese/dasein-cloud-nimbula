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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.AbstractVLANSupport;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.IpAddress;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.NICCreateOptions;
import org.dasein.cloud.network.NetworkInterface;
import org.dasein.cloud.network.NetworkServices;
import org.dasein.cloud.network.Networkable;
import org.dasein.cloud.network.RoutingTable;
import org.dasein.cloud.network.Subnet;
import org.dasein.cloud.network.SubnetCreateOptions;
import org.dasein.cloud.network.VLANState;
import org.dasein.cloud.network.VLANSupport;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.nimbula.NimbulaDirector;
import org.dasein.cloud.nimbula.NimbulaMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implements virtual ethernet networking in Nimbula.
 * @author George Reese (george.reese@enstratus.com)
 * @version 2012.09 modified for the 2012.09 API changes
 * @since unknown
 */
public class Vethernet extends AbstractVLANSupport {
    static private final Logger logger = NimbulaDirector.getLogger(VLANSupport.class);
    
    static public final String VDHCPD = "vdhcpd";
    
    private NimbulaDirector cloud;
    
    Vethernet(@Nonnull NimbulaDirector cloud) {
        super(cloud);
        this.cloud = cloud;
    }

    @Override
    public void addRouteToAddress(@Nonnull String toRoutingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String address) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables not currently supported");
    }

    @Override
    public void addRouteToGateway(@Nonnull String toRoutingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String gatewayId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables not currently supported");
    }

    @Override
    public void addRouteToNetworkInterface(@Nonnull String toRoutingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String nicId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("NICs not yet supported");
    }

    @Override
    public void addRouteToVirtualMachine(@Nonnull String toRoutingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables not currently supported");
    }

    @Override
    public boolean allowsNewNetworkInterfaceCreation() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsNewVlanCreation() throws CloudException, InternalException {
        ProviderContext ctx = cloud.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        return "root".equals(ctx.getAccountNumber());
    }

    private int findId() throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, "vethernet");
        
        method.list();
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
        
        method.list();
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
        int code = method.get(vlanId);
            
        if( code == 404 || code == 401 ) {
            return null;
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
    public boolean isNetworkInterfaceSupportEnabled() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull Iterable<VLAN> listVlans() throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, "vethernet");
        
        method.list();
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
    public void removeInternetGateway(@Nonnull String forVlanId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No internet gateway support");
    }

    @Override
    public void removeNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("NICs not yet supported");
    }

    @Override
    public void removeRoute(@Nonnull String inRoutingTableId, @Nonnull String destinationCidr) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables not yet supported");
    }

    @Override
    public void removeRoutingTable(@Nonnull String routingTableId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables not yet supported");
    }

    @Override
    public @Nonnull Subnet createSubnet(@Nonnull SubnetCreateOptions options) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Subnets are not supported");
    }
    
    
    @Override
    public @Nonnull VLAN createVlan(@Nonnull String cidr, @Nonnull String name, @Nonnull String description, @Nonnull String domainName, @Nonnull String[] dnsServers, @Nonnull String[] ntpServers) throws CloudException, InternalException {
        ProviderContext ctx = cloud.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        if( !"root".equals(ctx.getAccountNumber()) ) {
            throw new OperationNotSupportedException("VLAN creation is not yet supported for non-root");
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
        
        method.post(state);

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
        
        method.post(state);
        return vlan;
    }

    @Override
    public void detachNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("NICs not yet supported");
    }

    @Override
    public int getMaxNetworkInterfaceCount() throws CloudException, InternalException {
        return 0;
    }

    @Override
    public void removeVlan(String vlanId) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, "vethernet");
        
        method.delete(vlanId);

        JSONObject ob = findVdhcpd(vlanId);
        
        if( ob != null ) {
            method = new NimbulaMethod(cloud, "vdhcpd");
            try {
                method.delete(ob.getString("name"));
            }
            catch( JSONException e ) {
                if( logger.isDebugEnabled() ) {
                    logger.error("Error parsing JSON: " + e.getMessage());
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
        }
    }

    @Override
    public boolean supportsInternetGatewayCreation() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsRawAddressRouting() throws CloudException, InternalException {
        return false;
    }

    private void setNetwork(VLAN vlan) throws CloudException, InternalException {
        try {
            JSONObject ob = findVdhcpd(vlan.getProviderVlanId());
            
            if( ob != null ) {
                vlan.setDnsServers(new String[] { ob.getString("dns_server"), ob.getString("dns_server_standby") });
                vlan.setCidr(ob.getString("iprange_start") + "/" + ob.getString("iprange_mask"));
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

    private @Nullable VLAN toVlan(@Nullable JSONObject ob) throws JSONException, CloudException {
        if( ob == null ) {
            return null;
        }
        ProviderContext ctx = cloud.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request");
        }
        String name = ob.getString("name");
        VLAN vlan = new VLAN();
        String[] idInfo = cloud.parseId(name);

        vlan.setName(idInfo[2]);
        vlan.setDescription(idInfo[2]);
        vlan.setSupportedTraffic(new IPVersion[]{IPVersion.IPV4});
        vlan.setCurrentState(VLANState.AVAILABLE);
        if( ob.has("description") ) {
            vlan.setDescription(ob.getString("description"));
        }
        vlan.setProviderVlanId(name);
        vlan.setProviderOwnerId(idInfo[0]);
        vlan.setProviderRegionId(regionId);
        vlan.setProviderDataCenterId(vlan.getProviderRegionId() + "-a");
        return vlan;
    }

    @Override
    public boolean allowsNewSubnetCreation() throws CloudException, InternalException {
        return false;
    }

    @Override
    public void assignRoutingTableToSubnet(@Nonnull String subnetId, @Nonnull String routingTableId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables not supported");
    }

    @Override
    public void assignRoutingTableToVlan(@Nonnull String vlanId, @Nonnull String routingTableId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables not supported");
    }

    @Override
    public void attachNetworkInterface(@Nonnull String nicId, @Nonnull String vmId, int index) throws CloudException, InternalException {
        throw new OperationNotSupportedException("NICs tables not yet supported");
    }

    @Override
    public String createInternetGateway(@Nonnull String forVlanId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Internet gateways not supported");
    }

    @Override
    public @Nonnull String createRoutingTable(@Nonnull String forVlanId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables not currently supported");
    }

    @Override
    public @Nonnull NetworkInterface createNetworkInterface(@Nonnull NICCreateOptions options) throws CloudException, InternalException {
        throw new OperationNotSupportedException("NICs not yet supported");
    }

    @Override
    public @Nonnull String getProviderTermForNetworkInterface(@Nonnull Locale locale) {
        return "NIC";
    }

    @Override
    public @Nonnull String getProviderTermForSubnet(@Nonnull Locale locale) {
        return "subnet";
    }

    @Override
    public @Nonnull String getProviderTermForVlan(@Nonnull Locale locale) {
        return "vethernet";
    }

    @Override
    public NetworkInterface getNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public RoutingTable getRoutingTableForSubnet(@Nonnull String subnetId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public @Nonnull Requirement getRoutingTableSupport() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public RoutingTable getRoutingTableForVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public Subnet getSubnet(@Nonnull String subnetId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public @Nonnull Requirement getSubnetSupport() throws CloudException, InternalException {
        return Requirement.NONE;
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
    public @Nonnull Collection<String> listFirewallIdsForNIC(@Nonnull String nicId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listNetworkInterfaceStatus() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfaces() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfacesForVM(@Nonnull String forVmId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfacesInSubnet(@Nonnull String subnetId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfacesInVLAN(@Nonnull String vlanId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<Networkable> listResources(@Nonnull String inVlanId) throws CloudException, InternalException {
        ArrayList<Networkable> resources = new ArrayList<Networkable>();
        NetworkServices network = cloud.getNetworkServices();

        FirewallSupport fwSupport = network.getFirewallSupport();

        if( fwSupport != null ) {
            for( Firewall fw : fwSupport.list() ) {
                if( inVlanId.equals(fw.getProviderVlanId()) ) {
                    resources.add(fw);
                }
            }
        }

        IpAddressSupport ipSupport = network.getIpAddressSupport();

        if( ipSupport != null ) {
            for( IPVersion version : ipSupport.listSupportedIPVersions() ) {
                for( IpAddress addr : ipSupport.listIpPool(version, false) ) {
                    if( inVlanId.equals(addr.getProviderVlanId()) ) {
                        resources.add(addr);
                    }
                }

            }
        }
        for( RoutingTable table : listRoutingTables(inVlanId) ) {
            resources.add(table);
        }
        ComputeServices compute = cloud.getComputeServices();
        VirtualMachineSupport vmSupport = compute.getVirtualMachineSupport();
        Iterable<VirtualMachine> vms;

        if( vmSupport == null ) {
            vms = Collections.emptyList();
        }
        else {
            vms = vmSupport.listVirtualMachines();
        }
        for( Subnet subnet : listSubnets(inVlanId) ) {
            resources.add(subnet);
            for( VirtualMachine vm : vms ) {
                if( subnet.getProviderSubnetId().equals(vm.getProviderVlanId()) ) {
                    resources.add(vm);
                }
            }
        }
        return resources;
    }

    @Override
    public @Nonnull Iterable<RoutingTable> listRoutingTables(@Nonnull String inVlanId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<Subnet> listSubnets(@Nonnull String inVlanId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singletonList(IPVersion.IPV4);
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVlanStatus() throws CloudException, InternalException {
        ArrayList<ResourceStatus> status = new ArrayList<ResourceStatus>();

        for( VLAN vlan : listVlans() ) {
            status.add(new ResourceStatus(vlan.getProviderVlanId(), vlan.getCurrentState()));
        }
        return status;
    }

    @Override
    public void removeSubnet(String providerSubnetId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Subnets are not supported");
    }
}
