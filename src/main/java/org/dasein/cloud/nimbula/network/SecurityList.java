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
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.Direction;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallRule;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.Permission;
import org.dasein.cloud.network.Protocol;
import org.dasein.cloud.nimbula.NimbulaDirector;
import org.dasein.cloud.nimbula.NimbulaMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SecurityList implements FirewallSupport {
    static private final Logger logger = NimbulaDirector.getLogger(SecurityList.class);
    
    static public final String SECURITY_APPLICATION = "secapplication";
    static public final String SECURITY_IP_LIST     = "seciplist";
    static public final String SECURITY_LIST        = "seclist";
    static public final String SECURITY_RULES       = "secrule";
    
    private NimbulaDirector provider;
    
    SecurityList(@Nonnull NimbulaDirector provider) { this.provider = provider; }

    private String createApplication(Protocol protocol, String dport) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_APPLICATION);
        HashMap<String,Object> state = new HashMap<String,Object>();
        String name = protocol.name().toLowerCase();
        
        state.put("protocol", name);
        state.put("dport", dport);
        state.put("uri", null);
        name = (name + dport.replaceAll("-", "_"));
        state.put("name", provider.getNamePrefix() + "/dsn_" + name);
        state.put("icmptype", "");
        state.put("icmpcode", "");
        method.post(state);
        try {
            JSONObject ob = method.getResponseBody();
            
            return ob.getString("name");
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
    }
    
    private String createList(String cidr) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_IP_LIST);
        HashMap<String,Object> state = new HashMap<String,Object>();
        
        state.put("secipentries", Collections.singletonList(cidr));
        state.put("uri", null);
        state.put("name", provider.getNamePrefix() + "/dsn" + cidr.replaceAll("\\.", "_").replaceAll("/", "_"));
        method.post(state);
        try {
            JSONObject ob = method.getResponseBody();
            
            return ob.getString("name");
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
    }
    
    private String getApplicationId(Protocol protocol, int startPort, int endPort, boolean create) throws InternalException, CloudException {
        String dport = String.valueOf(startPort);
        
        if( endPort > startPort ) {
            dport = dport + "-" + endPort;
        }
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_APPLICATION);
        
        method.list();
        try {
            JSONArray array = method.getResponseBody().getJSONArray("result");
            
            for( int i=0; i<array.length(); i++ ) {
                JSONObject ob = array.getJSONObject(i);
                
                if( ob.has("protocol") && ob.getString("protocol").equals(protocol.name().toLowerCase()) ) {
                    if( ob.has("dport") && ob.getString("dport").equals(dport) ) {
                        return ob.getString("name");
                    }
                }
            }
            if( create ) {
                return createApplication(protocol, dport);
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
    
    private String getIpListId(String forCidr, boolean create) throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_IP_LIST);
        
        method.list();
        try {
            JSONArray array = method.getResponseBody().getJSONArray("result");
            String ipListId = null;
            
            for( int i=0; i<array.length(); i++ ) {
                JSONObject ob = array.getJSONObject(i);
                
                if( ob.has("secipentries") ) {
                    JSONArray entries = ob.getJSONArray("secipentries");
                    
                    if( entries.length() != 1 ) {
                        continue;
                    }
                    String entry = entries.getString(0);
                        
                    if( entry.equals(forCidr) ) {
                        ipListId = ob.getString("name");
                    }
                }
            }
            if( ipListId == null && create ) {
                ipListId = createList(forCidr);
            }
            return ipListId;
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
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        return authorize(firewallId, Direction.INGRESS, cidr, protocol, beginPort, endPort);
    }

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_RULES);
        String ipListId = getIpListId(cidr, true);
        String appId = getApplicationId(protocol, beginPort, endPort, true);

        String ruleId = (provider.getNamePrefix() + "/dsn_" + protocol.name() + "_" + System.currentTimeMillis() + "_" + beginPort + "_" + endPort);

        HashMap<String,Object> state = new HashMap<String,Object>();

        state.put("dst_list", "seclist:" + firewallId);
        state.put("src_list", "seciplist:" + ipListId);
        state.put("uri", null);
        state.put("application", appId);
        state.put("action", "PERMIT");
        state.put("name", ruleId);

        method.post(state);

        return ruleId;
    }

    private String toSecurityListName(String rawName) throws CloudException, InternalException {
        StringBuilder str = new StringBuilder();
        
        rawName = rawName.toLowerCase();
        for( int i=0; i<rawName.length(); i++ ) {
            char c = rawName.charAt(i);
            
            if( Character.isLetterOrDigit(c) ) {
                str.append(c);
            }
        }
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        if( str.length() > 0 ) {
            String acct = ctx.getAccountNumber();
            
            try {
                String user = new String(ctx.getAccessPublic(), "utf-8");
            
                return ("/" + acct + "/" + user + "/" + str.toString());
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
        }
        throw new CloudException("Invalid name: " + rawName);
    }
    
    @Override
    public @Nonnull String create(@Nonnull String name, @Nonnull String description) throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_LIST);
        HashMap<String,Object> state = new HashMap<String,Object>();
        
        state.put("policy", "");
        state.put("uri", null);
        state.put("outbound_cidr_policy", "");
        state.put("name", toSecurityListName(name));

        method.post(state);

        Firewall firewall;
        
        try {
            firewall = toFirewall(method.getResponseBody());
            if( firewall == null ) {
                throw new CloudException("No firewall was part of the response");
            }
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new CloudException(e);
        }
        //noinspection ConstantConditions
        return firewall.getProviderFirewallId();
    }

    @Override
    public @Nonnull String createInVLAN(@Nonnull String name, @Nonnull String description, @Nonnull String providerVlanId) throws InternalException, CloudException {
        throw new UnsupportedOperationException("VLAN security list creation is not supported");
    }
    
    @Override
    public void delete(@Nonnull String firewallId) throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_LIST);
        
        method.delete(firewallId);
    }

    @Override
    public @Nullable Firewall getFirewall(@Nonnull String firewallId) throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_LIST);
        int code = method.get(firewallId);
            
        if( code == 404 || code == 401 ) {
            return null;
        }
        try {
            return toFirewall(method.getResponseBody());
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
    public @Nonnull String getProviderTermForFirewall(@Nonnull Locale locale) {
        return "security list";
    }

    @Override
    public @Nonnull Collection<FirewallRule> getRules(@Nonnull String firewallId) throws InternalException, CloudException {
        // this needs refactoring
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_RULES);
        
        method.list();

        try {
            ArrayList<FirewallRule> rules = new ArrayList<FirewallRule>();
            JSONArray array = method.getResponseBody().getJSONArray("result");
            String id = "seclist:" + firewallId;
            
            for( int i=0; i<array.length(); i++ ) {
                JSONObject ob = array.getJSONObject(i);

                if( ob != null ) {
                    if( ob.has("dst_is_ip") && ob.getBoolean("dst_is_ip") ) {
                        continue;
                    }
                    if( ob.has("dst_list") && ob.getString("dst_list").equals(id) ) {
                        Collection<FirewallRule> r = toRule(firewallId, ob);
                        
                        if( r != null ) {
                            rules.addAll(r);
                        }
                    }
                }
            }
            return rules;
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
    public @Nonnull Collection<Firewall> list() throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_LIST);
        
        method.list();
        try {
            ArrayList<Firewall> firewalls = new ArrayList<Firewall>();
            JSONArray array = method.getResponseBody().getJSONArray("result");
            
            for( int i=0; i<array.length(); i++ ) {
                Firewall firewall = toFirewall(array.getJSONObject(i));
                
                if( firewall != null ) {
                    firewalls.add(firewall);
                }
            }
            return firewalls;
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
        return provider.getComputeServices().getVirtualMachineSupport().isSubscribed();
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private void revoke(String ruleId) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_RULES);
        
        method.delete(ruleId);
    }
    
    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        revoke(firewallId, Direction.INGRESS, cidr, protocol, beginPort, endPort);
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_RULES);
        String ipListId = getIpListId(cidr, false);
        String appId = getApplicationId(protocol, beginPort, endPort, false);

        if( ipListId == null || appId == null ) {
            return;
        }
        method.list();

        try {
            JSONArray array = method.getResponseBody().getJSONArray("result");
            String id = "seclist:" + firewallId;

            for( int i=0; i<array.length(); i++ ) {
                JSONObject ob = array.getJSONObject(i);

                if( ob != null ) {
                    if( ob.has("dst_is_ip") && ob.getBoolean("dst_is_ip") ) {
                        continue;
                    }
                    if( ob.has("dst_list") && ob.getString("dst_list").equals(id) ) {
                        if( ob.has("src_list") ) {
                            String listId = ob.getString("src_list");

                            if( listId.equals("seciplist:" + ipListId) ) {
                                if( ob.has("application") && ob.getString("application").equals(appId) ) {
                                    revoke(ob.getString("name"));
                                }
                            }
                        }
                    }
                }
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
    public boolean supportsRules(@Nonnull Direction direction, boolean inVlan) throws CloudException, InternalException {
        return (!inVlan && direction.equals(Direction.INGRESS));
    }

    private @Nullable Firewall toFirewall(@Nullable JSONObject ob) throws JSONException, CloudException {
        if( ob == null ) {
            return null;
        }
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request");
        }
        String id = (ob.has("name") ? ob.getString("name") : null);

        if( id == null ) {
            return null;
        }
        Firewall fw = new Firewall();

        fw.setProviderFirewallId(id);
        fw.setActive(true);
        fw.setAvailable(true);
        fw.setRegionId(regionId);

        String name = id;

        if( fw.getName() == null ) {
            String[] tmp = id.split("/");
            
            name = (tmp == null || tmp.length < 1) ? id : tmp[tmp.length-1];
            fw.setName(name);
        }
        if( fw.getDescription() == null ) {
            fw.setDescription(name);
        }
        // TODO: "outbound_cidr_policy": "PERMIT"
        // TODO: "policy": "DENY"
        return fw;
    }

    @SuppressWarnings("ConstantConditions")
    private Collection<FirewallRule> toRule(String firewallId, JSONObject ob) throws JSONException, CloudException, InternalException {
        String destList = (ob.has("dst_list") ? ob.getString("dst_list") : null);
        String appId = (ob.has("application") ? ob.getString("application") : null);
        FirewallRule rule = new FirewallRule();
        
        if( destList == null || appId == null ) {
            return null;
        }
        if( !ob.has("name") ) {
            return null;
        }
        rule.setFirewallId(firewallId);
        rule.setProviderRuleId(ob.getString("name"));
        rule.setPermission(Permission.ALLOW);
        if( ob.has("dst_is_ip") && ob.getBoolean("dst_is_ip") ) {
            rule.setDirection(Direction.EGRESS);
        }
        else {
            rule.setDirection(Direction.INGRESS);            
        }
        if( ob.has("action") ) {
            rule.setPermission(ob.getString("action").equalsIgnoreCase("permit") ? Permission.ALLOW : Permission.DENY);
        }
        JSONObject app = getSecurityApplication(appId);
        
        if( app == null ) {
            return null;
        }
        else {
            String protocol = app.getString("protocol");
            
            rule.setProtocol(Protocol.valueOf(protocol.toUpperCase()));
            if( app.has("dport") ) {
                String dport = app.getString("dport");
            
                if( dport != null && !dport.equals("") ) {
                    int idx = dport.indexOf('-');
                    
                    if( idx < 1 ) {
                        rule.setStartPort(Integer.parseInt(dport));
                        rule.setEndPort(Integer.parseInt(dport));
                    }
                    else {
                        String s = dport.substring(0,idx);
                        String e = dport.substring(idx+1);
                        
                        rule.setStartPort(Integer.parseInt(s));
                        rule.setStartPort(Integer.parseInt(e));
                    }
                }
            }
        }
        ArrayList<FirewallRule> rules = new ArrayList<FirewallRule>();
        
        if( ob.has("src_is_ip") && ob.getBoolean("src_is_ip") ) {
            String ipList = (ob.has("src_list") ? ob.getString("src_list") : null);
            
            if( ipList == null ) {
                return null;
            }
            JSONObject l = getSecurityIpList(ipList.substring("seciplist:".length()));
            
            if( l == null ) {
                return null;
            }
            JSONArray entries = (l.has("secipentries") ? l.getJSONArray("secipentries") : null);
            
            if( entries == null ) {
                return null;
            }
            for( int i=0; i<entries.length(); i++ ) {
                String entry = entries.getString(i);
                
                if( entry != null ) {
                    FirewallRule copy = new FirewallRule();
                    
                    copy.setCidr(entry);
                    copy.setDirection(rule.getDirection());
                    copy.setEndPort(rule.getEndPort());
                    copy.setFirewallId(rule.getFirewallId());
                    copy.setPermission(rule.getPermission());
                    copy.setProtocol(rule.getProtocol());
                    copy.setProviderRuleId(rule.getProviderRuleId() + ":" + copy.getCidr());
                    copy.setStartPort(rule.getStartPort());
                    rules.add(copy);
                }
            }
        }
        else {
            // TODO: implement me
        }
        return rules;
    }
    
    private JSONObject getSecurityApplication(String secIpListId) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_APPLICATION);
        int code = method.get(secIpListId);
            
        if( code == 404 || code == 401 ) {
            return null;
        }
        try {
            return method.getResponseBody();
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e);
        }          
    }
    
    private JSONObject getSecurityIpList(String secIpListId) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(provider, SECURITY_IP_LIST);
        int code = method.get(secIpListId);
            
        if( code == 404 || code == 401 ) {
            return null;
        }
        try {
            return method.getResponseBody();
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
