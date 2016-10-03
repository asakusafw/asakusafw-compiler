/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.dag.api.processor.basic;

import java.util.Map;
import java.util.Optional;

import com.asakusafw.dag.api.processor.ProcessorContext;

/**
 * A processor context which forwards method invocations to obtain properties and resources.
 * @since 0.4.0
 */
public interface ForwardProcessorContext extends ProcessorContext {

    /**
     * Returns the forwarding target.
     * @return the forwarding target
     */
    ProcessorContext getForward();

    @Override
    default ClassLoader getClassLoader() {
        return getForward().getClassLoader();
    }

    @Override
    default Map<String, String> getPropertyMap() {
        return getForward().getPropertyMap();
    }

    @Override
    default Optional<String> getProperty(String key) {
        return getForward().getProperty(key);
    }

    @Override
    default <T> Optional<T> getResource(Class<T> resourceType) {
        return getForward().getResource(resourceType);
    }

    @Override
    default ProcessorContext getDetached() {
        return getForward().getDetached();
    }
}
