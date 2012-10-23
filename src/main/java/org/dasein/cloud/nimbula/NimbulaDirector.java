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

package org.dasein.cloud.nimbula;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.nimbula.compute.NimbulaComputeServices;
import org.dasein.cloud.nimbula.network.NimbulaNetworkServices;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.StorageServices;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class NimbulaDirector extends AbstractCloud {
    static private @Nonnull String getLastItem(@Nonnull String name) {
        int idx = name.lastIndexOf('.');
        
        if( idx < 0 ) {
            return name;
        }
        else if( idx == (name.length()-1) ) {
            return "";
        }
        return name.substring(idx+1);
    }
    
    static public @Nonnull Logger getLogger(@Nonnull Class<?> cls) {
        String pkg = getLastItem(cls.getPackage().getName());
        
        if( pkg.equals("nimbula") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.nimbula.std." + pkg + getLastItem(cls.getName()));
    }
    
    static public @Nonnull Logger getWireLogger(@Nonnull Class<?> cls) {
        return Logger.getLogger("dasein.cloud.nimbula.wire." + getLastItem(cls.getPackage().getName()) + "." + getLastItem(cls.getName()));
    }
    
    static private final Logger logger = getLogger(NimbulaDirector.class);
    
    public NimbulaDirector() { }
    
    @Override
    public @Nonnull String getCloudName() {
        ProviderContext ctx = getContext();
        
        if( ctx == null ) {
            return "Private Nimbula Cloud";
        }
        String name = ctx.getCloudName();
        
        return (name == null ? "Private Nimbula Cloud" : name);
    }

    @Override
    public @Nonnull NimbulaComputeServices getComputeServices() {
        return new NimbulaComputeServices(this);
    }
    
    @Override
    public @Nonnull Site getDataCenterServices() {
        return new Site(this);
    }
    
    public @Nonnull String getNamePrefix() throws CloudException, InternalException {
        ProviderContext ctx = getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        try {
            String user = new String(ctx.getAccessPublic(), "utf-8");
            
            return ("/" + ctx.getAccountNumber() + "/" + user);
        }
        catch( UnsupportedEncodingException e ) {
            throw new InternalException(e);
        }
    }
    
    public @Nonnull NimbulaNetworkServices getNetworkServices() {
        return new NimbulaNetworkServices(this);
    }
    
    @Override
    public @Nonnull String getProviderName() {
        ProviderContext ctx = getContext();
        
        if( ctx == null ) {
            return "Nimbula";
        }
        String name = ctx.getCloudName();
        
        return (name == null ? "Nimbula" : name);
    }
    
    @Nonnull String getURL(@Nonnull String resource) throws CloudException {
        ProviderContext ctx = getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String endpoint = ctx.getEndpoint();
        
        if( endpoint == null ) {
            throw new CloudException("No context was set for this request");
        }
        if( endpoint.endsWith("/") ) {
            return endpoint + resource;
        }
        else {
            return endpoint + "/" + resource;
        }
    }
    
    public @Nonnull String[] parseId(@Nonnull String name) {
        int idx = name.indexOf("//");
        
        while( idx > -1 ) {
            name = name.replaceAll("//", "/");
        }
        while( name.startsWith("/") && name.length() > 1 ) {
            name = name.substring(1);
        }
        return name.split("/");
    }
    
    public @Nonnegative long parseTimestamp(@Nonnull String tsString) throws ParseException {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        
        return fmt.parse(tsString).getTime();
    }
    
    @Override
    public @Nullable String testContext() {
        try {
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                return null;
            }
            if( !getComputeServices().getVirtualMachineSupport().isSubscribed() ) {
                return null;
            }
            if( hasStorageServices() ) {
                // test the storage cloud if connected to one
                StorageServices services = getStorageServices();
                
                if( services != null && services.hasBlobStoreSupport() ) {
                    BlobStoreSupport support = services.getBlobStoreSupport();

                    if( support != null && !support.isSubscribed() ) {
                        return null;
                    }
                }
            }
            return ctx.getAccountNumber();
        }
        catch( Throwable t ) {
            logger.warn("Failed to test Nimbula context: " + t.getMessage());
            if( logger.isDebugEnabled() ) {
                t.printStackTrace();
            }
            return null;
        }
    }
}
