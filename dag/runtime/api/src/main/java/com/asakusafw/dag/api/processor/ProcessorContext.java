/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.api.processor;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * An abstract super interface of processing context for Asakusa DAG and its child elements.
 * @since 0.4.0
 */
public interface ProcessorContext {

    /**
     * Returns the current application class loader.
     * @return the class loader
     */
    ClassLoader getClassLoader();

    /**
     * Returns the context properties.
     * @return the context properties
     */
    default Map<String, String> getPropertyMap() {
        return Collections.emptyMap();
    }

    /**
     * Returns the generic context property value.
     * @param key the property key
     * @return the corresponded property value
     */
    default Optional<String> getProperty(String key) {
        Arguments.requireNonNull(key);
        return Optionals.get(getPropertyMap(), key);
    }

    /**
     * Returns a resource object.
     * @param <T> the resource type
     * @param resourceType the resource type
     * @return the corresponded resource object
     */
    default <T> Optional<T> getResource(Class<T> resourceType) {
        Arguments.requireNonNull(resourceType);
        return Optionals.empty();
    }

    /**
     * Returns a detached {@link ProcessorContext}.
     * This does not have any I/O information even if this object is subtype of {@link EdgeIoProcessorContext}.
     * @return the detached {@link ProcessorContext}
     */
    ProcessorContext getDetached();

    /**
     * Edits {@link ProcessorContext}.
     * @since 0.4.0
     */
    public interface Editor {

        /**
         * Adds a context property.
         * @param key the property key
         * @param value the property value
         * @see ProcessorContext#getProperty(String)
         */
        void addProperty(String key, String value);

        /**
         * Adds a context resource object.
         * @param <T> the resource type
         * @param type the resource type
         * @param object the resource object
         * @see ProcessorContext#getResource(Class)
         */
        <T> void addResource(Class<T> type, T object);
    }
}
