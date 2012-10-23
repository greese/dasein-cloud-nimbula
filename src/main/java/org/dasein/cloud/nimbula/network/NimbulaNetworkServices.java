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

import org.dasein.cloud.network.AbstractNetworkServices;
import org.dasein.cloud.nimbula.NimbulaDirector;

import javax.annotation.Nonnull;

public class NimbulaNetworkServices extends AbstractNetworkServices {
    private NimbulaDirector cloud;
    
    public NimbulaNetworkServices(@Nonnull NimbulaDirector cloud) { 
        this.cloud = cloud;
    }
    
    @Override
    public @Nonnull SecurityList getFirewallSupport() {
        return new SecurityList(cloud);
    }
    
    @Override
    public @Nonnull Vethernet getVlanSupport() {
        return new Vethernet(cloud);
    }
}
