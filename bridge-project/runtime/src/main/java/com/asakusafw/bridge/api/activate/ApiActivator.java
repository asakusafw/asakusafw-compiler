/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.bridge.api.activate;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import com.asakusafw.runtime.core.api.ApiStub;

/**
 * An abstract super interface which activates Asakusa Framework APIs.
 * @since 0.4.0
 */
@FunctionalInterface
public interface ApiActivator {

    /**
     * Returns whether or not the target API is available.
     * @return if {@code true} if the target API is available, otherwise {@code false}
     */
    default boolean isAvailable() {
        return true;
    }

    /**
     * Activates the target API.
     * @return the reference of API implementation
     */
    ApiStub.Reference<?> activate();

    /**
     * Returns {@link ApiActivator} instances via SPI.
     * @param classLoader the service class loader
     * @return the loaded initializers
     */
    static List<ApiActivator> load(ClassLoader classLoader) {
        List<ApiActivator> results = new ArrayList<>();
        for (ApiActivator activator : ServiceLoader.load(ApiActivator.class, classLoader)) {
            if (activator.isAvailable()) {
                results.add(activator);
            }
        }
        return results;
    }
}
