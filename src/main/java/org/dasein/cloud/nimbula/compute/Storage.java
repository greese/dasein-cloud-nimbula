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

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.AbstractVolumeSupport;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.compute.VolumeCreateOptions;
import org.dasein.cloud.compute.VolumeFormat;
import org.dasein.cloud.compute.VolumeProduct;
import org.dasein.cloud.compute.VolumeState;
import org.dasein.cloud.compute.VolumeType;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.nimbula.NimbulaDirector;
import org.dasein.cloud.nimbula.NimbulaMethod;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.UUID;

/**
 * Implements Nimbula disk storage support according to the Dasein Cloud volume API.
 * <p>Created by George Reese: 10/23/12 11:26 AM</p>
 * @author George Reese
 * @version 2012.09.1 initial version
 * @since 2012.09.1
 */
public class Storage extends AbstractVolumeSupport {
    static private final Logger logger = NimbulaDirector.getLogger(Storage.class);

    static public final String STORAGE_ATTACHMENT  = "storageattachment";
    static public final String STORAGE_PROPERTY    = "property/storage";
    static public final String STORAGE_VOLUME      = "storagevolume";

    static private class Attachment {
        public String id;
        public String server;
        public int index;
    }

    private NimbulaDirector provider;

    public Storage(@Nonnull NimbulaDirector provider) {
        super(provider);
        this.provider = provider;
    }

    @Override
    public void attach(@Nonnull String volumeId, @Nonnull String toServer, @Nonnull String deviceId) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + Storage.class.getName() + ".attach(" + volumeId + "," + toServer + "," + deviceId + ")");
        }
        try {
            VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(toServer);

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + toServer);
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was specified for this request");
            }
            NimbulaMethod method = new NimbulaMethod(provider, STORAGE_ATTACHMENT);
            HashMap<String,Object> state = new HashMap<String,Object>();

            state.put("storage_volume_name", volumeId);
            state.put("instance_name", toServer);
            state.put("index", deviceId);
            int code = method.post(state);

            if( code == 401 ) {
                throw new CloudException("Invalid server " + toServer + " or invalid volume " + volumeId);
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + Storage.class.getName() + ".attach()");
            }
        }
    }

    @Override
    public @Nonnull String createVolume(@Nonnull VolumeCreateOptions options) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was specified for this request");
        }
        org.dasein.util.uom.storage.Storage<Megabyte> mb;

        mb = (org.dasein.util.uom.storage.Storage<Megabyte>)options.getVolumeSize().convertTo(org.dasein.util.uom.storage.Storage.MEGABYTE);

        NimbulaMethod method = new NimbulaMethod(provider, STORAGE_VOLUME);
        HashMap<String,Object> state = new HashMap<String,Object>();

        state.put("name", provider.getNamePrefix() + "/" + UUID.randomUUID().toString());
        state.put("size_Mbytes", String.valueOf(mb.intValue()));

        String productId = options.getVolumeProductId();

        if( productId != null ) {
            state.put("properties", productId);
        }
        state.put("status", "Online");
        method.post(state);

        try {
            Volume volume = toVolume(ctx, getAttachmentList(), method.getResponseBody());

            if( volume == null ) {
                throw new CloudException("No volume was part of the response");
            }
            return volume.getProviderVolumeId();
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public void detach(@Nonnull String volumeId, boolean force) throws InternalException, CloudException {
        Attachment attachment = getAttachment(getAttachmentList(), volumeId);

        if( attachment == null ) {
            throw new CloudException("The volume " + volumeId + " is not attached");
        }
        NimbulaMethod method = new NimbulaMethod(provider, STORAGE_ATTACHMENT);

        method.delete(attachment.id);
    }

    private @Nonnull JSONArray getAttachmentList() throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was specified with this request");
        }
        NimbulaMethod method = new NimbulaMethod(provider, STORAGE_ATTACHMENT);

        method.list();
        try {
            return method.getResponseBody().getJSONArray("result");
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e);
        }
    }

    private @Nullable Attachment getAttachment(@Nonnull JSONArray rawAttachments, @Nonnull String volumeId) throws CloudException, InternalException {
        try {
            for( int i=0; i<rawAttachments.length(); i++ ) {
                JSONObject json = rawAttachments.getJSONObject(i);

                if( json.has("storage_volume_name") && json.getString("storage_volume_name").equals(volumeId) ) {
                    Attachment a = new Attachment();

                    a.id = (json.has("name") ? json.getString("name") : null);
                    a.server = (json.has("instance_name") ? json.getString("instance_name") : null);
                    a.index = (json.has("index") ? json.getInt("index") : 1);
                    if( a.id == null || a.server == null ) {
                        return null;
                    }
                    return a;
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
    public int getMaximumVolumeCount() throws InternalException, CloudException {
        return -2;
    }

    @Override
    public org.dasein.util.uom.storage.Storage<Gigabyte> getMaximumVolumeSize() throws InternalException, CloudException {
        return new org.dasein.util.uom.storage.Storage<Gigabyte>(5, org.dasein.util.uom.storage.Storage.GIGABYTE);
    }

    @Override
    public @Nonnull org.dasein.util.uom.storage.Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        return new org.dasein.util.uom.storage.Storage<Gigabyte>(1, org.dasein.util.uom.storage.Storage.GIGABYTE);
    }

    @Override
    public @Nonnull String getProviderTermForVolume(@Nonnull Locale locale) {
        return "storage volume";
    }

    @Override
    public Volume getVolume(@Nonnull String volumeId) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was specified for this request");
        }
        NimbulaMethod method = new NimbulaMethod(provider, STORAGE_VOLUME);
        int code = method.get(volumeId);

        if( code == 404 || code == 401 ) {
            return null;
        }
        try {
            return toVolume(ctx, getAttachmentList(), method.getResponseBody());
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
    public @Nonnull Requirement getVolumeProductRequirement() throws InternalException, CloudException {
        return Requirement.OPTIONAL;
    }

    @Override
    public boolean isVolumeSizeDeterminedByProduct() throws InternalException, CloudException {
        return false;
    }

    static private ArrayList<String> deviceIds;

    @Override
    public @Nonnull Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        if( deviceIds == null ) {
            ArrayList<String> ids = new ArrayList<String>();

            for( int i=1; i<20; i++ ) {
                ids.add(String.valueOf(i));
            }
            deviceIds = ids;
        }
        return deviceIds;
    }

    @Override
    public @Nonnull Iterable<VolumeFormat> listSupportedFormats() throws InternalException, CloudException {
        return Collections.singletonList(VolumeFormat.BLOCK);
    }

    @Override
    public @Nonnull Iterable<VolumeProduct> listVolumeProducts() throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(provider, STORAGE_PROPERTY);

        method.list();
        try {
            ArrayList<VolumeProduct> products = new ArrayList<VolumeProduct>();
            JSONArray array = method.getResponseBody().getJSONArray("result");

            for( int i=0; i<array.length(); i++ ) {
                VolumeProduct product = toProduct(array.getJSONObject(i));

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
    public @Nonnull Iterable<ResourceStatus> listVolumeStatus() throws InternalException, CloudException {
        ArrayList<ResourceStatus> status = new ArrayList<ResourceStatus>();

        for( Volume v : listVolumes() ) {
            status.add(new ResourceStatus(v.getProviderVolumeId(), v.getCurrentState()));
        }
        return status;
    }

    @Override
    public @Nonnull Iterable<Volume> listVolumes() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was specified with this request");
        }
        NimbulaMethod method = new NimbulaMethod(provider, STORAGE_VOLUME);

        method.list();
        try {
            JSONArray attachmentList = getAttachmentList();
            ArrayList<Volume> volumes = new ArrayList<Volume>();
            JSONArray array = method.getResponseBody().getJSONArray("result");

            for( int i=0; i<array.length(); i++ ) {
                Volume volume = toVolume(ctx, attachmentList, array.getJSONObject(i));

                if( volume != null ) {
                    volumes.add(volume);
                }
            }
            return volumes;
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
    public void remove(@Nonnull String volumeId) throws InternalException, CloudException {
        NimbulaMethod method = new NimbulaMethod(provider, STORAGE_VOLUME);

        method.delete(volumeId);
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nonnull VolumeState toState(@Nonnull String status) {
        if( status.equalsIgnoreCase("online") ) {
            return VolumeState.AVAILABLE;
        }
        String[] parts = status.split(",");

        if( parts == null || parts.length < 1 ) {
            parts = new String[] { status };
        }
        for( String p : parts ) {
            if( p.equalsIgnoreCase("adding") ) {
                return VolumeState.PENDING;
            }
            if( p.equalsIgnoreCase("attached") ) {
                return VolumeState.AVAILABLE;
            }
        }
        logger.warn("DEBUG: Unknown Nimbula volume state: " + status);
        return VolumeState.PENDING;
    }

    private @Nullable VolumeProduct toProduct(@Nonnull JSONObject json) throws CloudException, InternalException {
        try {
            String name = (json.has("name") ? json.getString("name") : null);

            if( name == null ) {
                return null;
            }
            return VolumeProduct.getInstance(name, name, name, VolumeType.HDD);
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    private @Nullable Volume toVolume(@Nonnull ProviderContext ctx, @Nonnull JSONArray attachmentList, @Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was specified for this request");
        }
        Volume volume = new Volume();

        volume.setCurrentState(VolumeState.PENDING);
        volume.setProviderDataCenterId(regionId);
        volume.setProviderRegionId(regionId);
        volume.setType(VolumeType.HDD);
        try {
            if( json.has("name") ) {
                volume.setProviderVolumeId(json.getString("name"));
            }
            if( json.has("status") ) {
                String status = json.getString("status");

                volume.setCurrentState(toState(status));
                if( status.contains("Attached") ) {
                    Attachment attachment = getAttachment(attachmentList, volume.getProviderVolumeId());

                    if( attachment != null ){
                        volume.setProviderVirtualMachineId(attachment.server);
                        volume.setDeviceId(String.valueOf(attachment.index));
                    }
                }
            }
            if( json.has("size_mbytes") ) {
                volume.setSize(new org.dasein.util.uom.storage.Storage<Megabyte>(json.getInt("size_mbytes"), org.dasein.util.uom.storage.Storage.MEGABYTE));
            }
            if( json.has("properties") ) {
                volume.setProviderProductId(json.getString("properties"));
            }
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
        if( volume.getProviderVolumeId() == null ) {
            return null;
        }
        if( volume.getName() == null ) {
            volume.setName(volume.getProviderVolumeId());
        }
        if( volume.getDescription() == null ) {
            volume.setDescription(volume.getName());
        }
        return volume;
    }
}
