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
package com.asakusafw.bridge.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.runtime.core.ResourceConfiguration;

/**
 * Dummy implementation of {@link ResourceConfiguration}.
 */
public class MapConfiguration implements ResourceConfiguration {

    private final Map<String, String> entities;

    private final ClassLoader loader;

    /**
     * Creates a new empty instance.
     */
    public MapConfiguration() {
        this(Collections.emptyMap(), MapConfiguration.class.getClassLoader());
    }

    /**
     * Creates a new instance.
     * @param entities the initial configurations
     * @param loader the class loader
     */
    public MapConfiguration(Map<String, String> entities, ClassLoader loader) {
        this.entities = new LinkedHashMap<>(entities);
        this.loader = loader;
    }

    @Override
    public String get(String keyName, String defaultValue) {
        String value = entities.get(keyName);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public void set(String keyName, String value) {
        entities.put(keyName, value);
    }

    @Override
    public ClassLoader getClassLoader() {
        return loader;
    }
}
