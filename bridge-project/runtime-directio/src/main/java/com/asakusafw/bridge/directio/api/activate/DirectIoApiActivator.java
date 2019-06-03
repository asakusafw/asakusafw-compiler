/**
 * Copyright 2011-2019 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.bridge.directio.api.activate;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.api.activate.ApiActivator;
import com.asakusafw.bridge.directio.api.DirectIo;
import com.asakusafw.runtime.core.api.ApiStub;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.runtime.directio.api.DirectIoApi;
import com.asakusafw.runtime.io.ModelInput;

/**
 * Activates {@link DirectIo}.
 * @since 0.4.0
 */
@SuppressWarnings("deprecation")
public class DirectIoApiActivator implements ApiActivator {

    static final Logger LOG = LoggerFactory.getLogger(DirectIoApiActivator.class);

    private static final String API_CLASS = "com.asakusafw.runtime.directio.api.DirectIoApi";

    private static final boolean AVAILABLE;
    static {
        boolean available = false;
        try {
            Class.forName(API_CLASS);
            available = true;
        } catch (ReflectiveOperationException e) {
            LOG.trace("Direct I/O API is not available", e);
        }
        AVAILABLE = available;
    }

    @Override
    public boolean isAvailable() {
        return AVAILABLE;
    }

    @Override
    public ApiStub.Reference<?> activate() {
        return com.asakusafw.runtime.directio.api.DirectIo.getStub().activate(Lazy.API);
    }

    private static final class Lazy {

        private Lazy() {
            return;
        }

        static final DirectIoApi API = new DirectIoApi() {
            @Override
            public <T> ModelInput<T> open(
                    Class<? extends DataFormat<T>> formatClass,
                    String basePath, String resourcePattern) throws IOException {
                return DirectIo.open(formatClass, basePath, resourcePattern);
            }
        };
    }
}
