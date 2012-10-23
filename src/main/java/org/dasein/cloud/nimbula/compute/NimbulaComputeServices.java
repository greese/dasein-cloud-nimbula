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

import org.dasein.cloud.compute.AbstractComputeServices;
import org.dasein.cloud.nimbula.NimbulaDirector;

import javax.annotation.Nonnull;

public class NimbulaComputeServices extends AbstractComputeServices {
    private NimbulaDirector cloud;
    
    public NimbulaComputeServices(@Nonnull NimbulaDirector cloud) {
        this.cloud = cloud;
    }
    
    @Override
    public @Nonnull Image getImageSupport() {
        return new Image(cloud);
    }
    
    @Override
    public @Nonnull Instance getVirtualMachineSupport() {
        return new Instance(cloud);
    }
}
