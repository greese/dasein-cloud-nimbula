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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Site implements DataCenterServices {
    private NimbulaDirector cloud = null;
    
    public Site(@Nonnull NimbulaDirector cloud) {
        this.cloud = cloud;
    }
    
    @Override
    public @Nullable DataCenter getDataCenter(@Nonnull String dcId) throws InternalException, CloudException {
        ProviderContext ctx = cloud.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String regionId = ctx.getRegionId();
        
        if( regionId == null ) {
            throw new CloudException("No region was set for this context");
        }
        for( DataCenter dc : listDataCenters(regionId) ) {
            if( dcId.equals(dc.getProviderDataCenterId()) ) {
                return dc;
            }
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForDataCenter(@Nonnull Locale locale) {
        return "data center";
    }

    @Override
    public @Nonnull String getProviderTermForRegion(@Nonnull Locale locale) {
        return "site";
    }

    @Override
    public @Nullable Region getRegion(@Nonnull String siteId) throws InternalException, CloudException {
        for( Region region : listRegions() ) {
            if( region.getProviderRegionId().equals(siteId) ) {
                return region;
            }
        }
        return null;
    }

    @Override
    public @Nonnull Collection<DataCenter> listDataCenters(@Nonnull String siteId) throws InternalException, CloudException {
        Region region = getRegion(siteId);
        
        if( region == null ) {
            throw new CloudException("No such region: " + siteId);
        }
        DataCenter dc = new DataCenter();
            
        dc.setActive(true);
        dc.setAvailable(true);
        dc.setName(region.getName() + " (DC)");
        dc.setProviderDataCenterId(region.getProviderRegionId());
        dc.setRegionId(region.getProviderRegionId());
        return Collections.singletonList(dc);
    }

    @Override
    public @Nonnull Collection<Region> listRegions() throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(cloud, "info");

        method.list();
        try {
            ArrayList<Region> regions = new ArrayList<Region>();
            JSONArray array = method.getResponseBody().getJSONArray("result");

            for( int i=0; i<array.length(); i++ ) {
                Region region = toRegion(array.getJSONObject(i));

                if( region != null ) {
                    regions.add(region);
                }
            }
            return regions;
        }
        catch( JSONException e ) {
            throw new InternalException(e);
        }
    }
    
    private @Nullable Region toRegion(@Nullable JSONObject json) throws CloudException {
        if( json == null ) {
            return null;
        }
        try {
            String name = (json.has("name") ? json.getString("name") : null);
    
            if( name == null || name.length() < 1 ) {
                return null;
            }
            Region region = new Region();
    
            region.setActive(true);
            region.setAvailable(true);
            region.setJurisdiction("US");
            region.setName(name);
            region.setProviderRegionId(name);
            return region;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }
}
