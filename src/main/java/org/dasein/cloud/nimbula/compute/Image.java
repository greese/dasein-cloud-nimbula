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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.compute.AbstractImageSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageFilterOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.nimbula.NimbulaDirector;
import org.dasein.cloud.nimbula.NimbulaMethod;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Image extends AbstractImageSupport {
    static private final Logger logger = NimbulaDirector.getLogger(Image.class);
    
    static public final String IMAGELIST    = "imagelist";
    static public final String MACHINEIMAGE = "machineimage";
    
    private NimbulaDirector cloud;
    
    Image(@Nonnull NimbulaDirector cloud) {
        super(cloud);
        this.cloud = cloud;
    }

    @Override
    public MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, MACHINEIMAGE);
        int code = method.get(providerImageId);

        if( code == 404 || code == 401 ) {
            return null;
        }
        try {
            return toMachineImage(method.getResponseBody());
        }
        catch( JSONException e ) {
            if( logger.isDebugEnabled() ) {
                logger.error("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
            throw new InternalException(e);
        }
    }

    public @Nullable String getMachineImageId(@Nonnull String imagelist, @Nonnegative int entryNumber) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, Image.IMAGELIST);
        
        method.get(imagelist);
        try {
            JSONObject item = method.getResponseBody();
            JSONArray entries = item.getJSONArray("entries");

            for( int i=0; i<entries.length(); i++ ) {
                JSONObject entry = entries.getJSONObject(i);
                JSONArray images = entry.getJSONArray("machineimages");

                if( images.length() >= entryNumber ) {
                    return images.getString(entryNumber-1);
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
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return "image";
    }

    @Override
    public boolean hasPublicLibrary() {
        return true;
    }

    @Override
    public @Nonnull Requirement identifyLocalBundlingRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        return machineImageId.startsWith("/nimbula/public");
    }
    
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        ProviderContext ctx = getProvider().getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String acct = (options == null ? null : options.getAccountNumber());

        if( acct == null || acct.equals(ctx.getAccountNumber()) ) {
            NimbulaMethod method = new NimbulaMethod(cloud, MACHINEIMAGE);

            method.list();
            try {
                ArrayList<MachineImage> images = new ArrayList<MachineImage>();
                JSONArray array = method.getResponseBody().getJSONArray("result");

                for( int i=0; i<array.length(); i++ ) {
                    MachineImage image = toMachineImage(array.getJSONObject(i));

                    if( image != null && (options == null || options.matches(image)) ) {
                        images.add(image);
                    }
                }
                return images;
            }
            catch( JSONException e ) {
                if( logger.isDebugEnabled() ) {
                    logger.error("Error parsing JSON: " + e.getMessage());
                    e.printStackTrace();
                }
                throw new InternalException(e);
            }
        }
        else {
            if( !acct.endsWith("/") ){
                acct = acct + "/";
            }
            NimbulaMethod method = new NimbulaMethod(cloud, MACHINEIMAGE);
            int code = method.get(acct);

            if( code == 401 ) {
                return Collections.emptyList();
            }
            try {
                ArrayList<MachineImage> images = new ArrayList<MachineImage>();
                JSONArray array = method.getResponseBody().getJSONArray("result");

                for( int i=0; i<array.length(); i++ ) {
                    MachineImage image = toMachineImage(array.getJSONObject(i));

                    if( image != null && options.matches(image) ) {
                        images.add(image);
                    }
                }
                return images;
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

    @Override
    @Deprecated
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(@Nullable String accountId) throws CloudException, InternalException {
        if( accountId == null ) {
            accountId = "/nimbula/public/";
        }
        return listImages(ImageClass.MACHINE, accountId);
    }

    @Override
    public @Nonnull Iterable<ImageClass> listSupportedImageClasses() throws CloudException, InternalException {
        return Collections.singletonList(ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImageType> listSupportedImageTypes() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageType.VOLUME);
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.NIMBULA);
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormatsForBundling() throws CloudException, InternalException {
        throw new OperationNotSupportedException("Bundles not supported");
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void remove(@Nonnull String machineImageId) throws CloudException, InternalException {
        remove(machineImageId, false);
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        NimbulaMethod method = new NimbulaMethod(cloud, MACHINEIMAGE);

        method.delete(providerImageId);
    }

    @Override
    public void removeAllImageShares(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Sharing is not supported");
    }

    @Override
    public void removeImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Sharing is not supported");
    }

    @Override
    public void removePublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Sharing is not supported");
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        return searchImages(null, keyword, platform, architecture, ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchPublicImages(final @Nonnull ImageFilterOptions options) throws CloudException, InternalException {
        PopulatorThread<MachineImage> populator;

        cloud.hold();
        populator = new PopulatorThread<MachineImage>(new JiteratorPopulator<MachineImage>() {
            @Override
            public void populate(@Nonnull Jiterator<MachineImage> iterator) throws Exception {
                try {
                    for( MachineImage image : listImages(ImageFilterOptions.getInstance().withAccountNumber("/nimbula/public/")) ) {
                        if( options.matches(image) ) {
                            iterator.push(image);
                        }
                    }
                }
                finally {
                    cloud.release();
                }
            }
        });
        populator.populate();
        return populator.getResult();
    }

    @Override
    public boolean supportsCustomImages() {
        return false;
    }

    @Override
    public boolean supportsDirectImageUpload() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageCapture(@Nonnull MachineImageType type) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageSharing() {
        return false;
    }

    @Override
    public boolean supportsImageSharingWithPublic() {
        return false;
    }

    @Override
    public boolean supportsPublicLibrary(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return cls.equals(ImageClass.MACHINE);
    }

    private @Nullable MachineImage toMachineImage(@Nonnull JSONObject ob) throws JSONException, CloudException {
        ProviderContext ctx = cloud.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was set for this request");
        }
        if( !ob.has("name") ) {
            return null;
        }
        MachineImage image = new MachineImage();
        String name = ob.getString("name");
        String[] idInfo = cloud.parseId(name);
        
        image.setProviderOwnerId(idInfo[0]);
        image.setProviderMachineImageId(name);
        image.setName(idInfo[2]);
        image.setImageClass(ImageClass.MACHINE);
        Platform platform = Platform.UNKNOWN;
        try {
            JSONObject attrs = ob.getJSONObject("attributes");
            
            platform = Platform.guess(attrs.getString("type"));
            image.setDescription(idInfo[2] + " (" + attrs.getString("type") + ")");
        }
        catch( Throwable ignore ) {
            image.setDescription(idInfo[2]);
        }
        image.setPlatform(platform);
        image.setArchitecture(Architecture.I64);
        image.setCurrentState(MachineImageState.ACTIVE);
        image.setProviderRegionId(regionId);
        image.setSoftware("");
        image.setType(MachineImageType.VOLUME);
        return image;
    }
}
