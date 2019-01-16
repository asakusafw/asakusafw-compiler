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
package com.asakusafw.lang.compiler.common;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A basic implementation of {@link AttributeContainer}.
 * Clients can inherit this class.
 */
public class BasicAttributeContainer implements AttributeContainer {

    private final Map<Class<?>, Object> attributes = new LinkedHashMap<>();

    @Override
    public Collection<Class<?>> getAttributeTypes() {
        return Collections.unmodifiableSet(attributes.keySet());
    }

    @Override
    public <T> T getAttribute(Class<T> type) {
        Object value = attributes.get(type);
        if (value == null) {
            return null;
        }
        return type.cast(value);
    }

    @Override
    public <T> void putAttribute(Class<T> type, T value) {
        if (value == null) {
            attributes.remove(type);
        } else {
            attributes.put(type, value);
        }
    }
}
