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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * An abstract implementation of {@link ProcessorContext}.
 * @param <S> the self type
 * @since 0.4.0
 */
public abstract class AbstractProcessorContext<S extends AbstractProcessorContext<S>> implements ProcessorContext {

    private final Map<String, String> properties = new LinkedHashMap<>();

    private final Map<Class<?>, Object> resources = new LinkedHashMap<>();

    /**
     * Returns this object.
     * @return this
     */
    protected abstract S self();

    @Override
    public Map<String, String> getPropertyMap() {
        return Collections.unmodifiableMap(properties);
    }

    /**
     * Returns the resource map.
     * @return the resource map
     */
    public Map<Class<?>, Object> getResourceMap() {
        return Collections.unmodifiableMap(resources);
    }

    @Override
    public final Optional<String> getProperty(String key) {
        Arguments.requireNonNull(key);
        return Optionals.get(properties, key);
    }

    /**
     * Adds or removes a property entry.
     * @param key the property key
     * @param value the property value, or {@code null} to remove the entry
     * @return this
     */
    public final S withProperty(String key, String value) {
        Arguments.requireNonNull(key);
        if (value == null) {
            properties.remove(key);
        } else {
            properties.put(key, value);
        }
        return self();
    }

    @Override
    public final <T> Optional<T> getResource(Class<T> resourceType) {
        Arguments.requireNonNull(resourceType);
        return Optionals.get(resources, resourceType).map(resourceType::cast);
    }

    /**
     * Adds or removes a resource entry.
     * @param <T> the resource type
     * @param resourceType the resource type
     * @param resourceObject the resource object, or {@code null} to remove the entry
     * @return this
     */
    public final <T> S withResource(Class<T> resourceType, Object resourceObject) {
        Arguments.requireNonNull(resourceType);
        if (resourceObject == null) {
            resources.remove(resourceType);
        } else {
            resources.put(resourceType, resourceObject);
        }
        return self();
    }

    /**
     * Adds a resource entry.
     * @param resourceObject the resource object, or {@code null} to remove the entry
     * @return this
     */
    public final S withResource(Object resourceObject) {
        Arguments.requireNonNull(resourceObject);
        Class<?> theClass = resourceObject.getClass();
        Arguments.require(theClass.isAnonymousClass() == false);
        Arguments.require(theClass.isLocalClass() == false);
        return withResource(theClass, resourceObject);
    }

    @Override
    public final ProcessorContext getDetached() {
        return new Detached(getClassLoader(), properties, resources);
    }

    /**
     * Returns editor for this.
     * @return the editor
     */
    public Editor getEditor() {
        return new Editor() {
            @Override
            public void addProperty(String key, String value) {
                withProperty(key, value);
            }
            @Override
            public <T> void addResource(Class<T> type, T object) {
                withResource(type, object);
            }
        };
    }

    private static final class Detached implements ProcessorContext {

        private final ClassLoader classLoader;

        private final Map<String, String> properties;

        private final Map<Class<?>, ?> resources;

        Detached(ClassLoader classLoader, Map<String, String> properties, Map<Class<?>, ?> resources) {
            this.classLoader = classLoader;
            this.properties = Arguments.freeze(properties);
            this.resources = Arguments.freeze(resources);
        }

        @Override
        public ClassLoader getClassLoader() {
            return classLoader;
        }

        @Override
        public Map<String, String> getPropertyMap() {
            return properties;
        }

        @Override
        public <T> Optional<T> getResource(Class<T> resourceType) {
            Arguments.requireNonNull(resourceType);
            return Optionals.get(resources, resourceType).map(resourceType::cast);
        }

        @Override
        public ProcessorContext getDetached() {
            return this;
        }
    }
}
