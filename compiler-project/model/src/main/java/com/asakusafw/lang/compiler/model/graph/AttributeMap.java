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
package com.asakusafw.lang.compiler.model.graph;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.asakusafw.lang.compiler.common.AttributeEntry;

/**
 * Represents a map of attributes.
 * @since 0.4.1
 */
public final class AttributeMap {

    static final AttributeMap EMPTY = new AttributeMap(Collections.emptyMap());

    private final Map<Class<?>, Object> entries;

    private AttributeMap(Map<Class<?>, Object> entries) {
        this.entries = shrink(entries);
    }

    private static Map<Class<?>, Object> shrink(Map<Class<?>, Object> map) {
        switch (map.size()) {
        case 0:
            return Collections.emptyMap();
        case 1:
            Map.Entry<Class<?>, Object> entry = map.entrySet().iterator().next();
            return Collections.singletonMap(entry.getKey(), entry.getValue());
        default:
            return map;
        }
    }

    AttributeMap(Builder builder) {
        this(builder.entries());
    }

    /**
     * Returns the all attribute types which this map has.
     * @return the all attribute types
     */
    public Set<Class<?>> getAttributeTypes() {
        if (entries.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(entries.keySet());
    }

    /**
     * Returns an attribute.
     * @param <T> the attribute type
     * @param type the attribute type
     * @return the attribute value, or {@code null} if the map has no such an attribute
     */
    public <T> T getAttribute(Class<T> type) {
        return type.cast(entries.get(type));
    }

    AttributeMap copy() {
        if (entries.isEmpty()) {
            return this;
        }
        Map<Class<?>, Object> copy = new LinkedHashMap<>();
        copyTo(entries, copy);
        return new AttributeMap(copy);
    }

    void copyTo(AttributeMap.Builder builder) {
        copyTo(entries, builder.entries());
    }

    static void copyTo(Map<Class<?>, Object> from, Map<Class<?>, Object> to) {
        for (Map.Entry<Class<?>, Object> entry : from.entrySet()) {
            Class<?> key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof OperatorAttribute) {
                value = ((OperatorAttribute) value).copy();
                assert key.isInstance(value);
            }
            to.put(key, value);
        }
    }

    @Override
    public String toString() {
        if (entries == null) {
            return "{}"; //$NON-NLS-1$
        }
        return entries.entrySet().stream()
                .sequential()
                .map(e -> e.getKey().getSimpleName() + "=" + e.getValue()) //$NON-NLS-1$
                .collect(Collectors.joining(
                        ", ", //$NON-NLS-1$
                        "{ ", //$NON-NLS-1$
                        " }")); //$NON-NLS-1$
    }

    /**
     * A builder of {@link AttributeMap}.
     * @since 0.4.1
     */
    public static class Builder {

        private Map<Class<?>, Object> entries;

        Map<Class<?>, Object> entries() {
            if (entries == null) {
                entries = new LinkedHashMap<>();
            }
            return entries;
        }

        /**
         * Adds an attribute.
         * @param entry the attribute entry
         * @return this
         */
        public Builder add(AttributeEntry<?> entry) {
            Objects.requireNonNull(entry);
            entries().put(entry.getType(), entry.getValue());
            return this;
        }

        /**
         * Adds an attribute.
         * @param value the attribute value
         * @return this
         */
        public Builder add(Enum<?> value) {
            Objects.requireNonNull(value);
            entries().put(value.getDeclaringClass(), value);
            return this;
        }

        /**
         * Adds an attribute.
         * @param <T> the attribute type
         * @param type the attribute type
         * @param object the attribute value
         * @return this
         */
        public <T> Builder add(Class<T> type, T object) {
            Objects.requireNonNull(type);
            Objects.requireNonNull(object);
            entries().put(type, object);
            return this;
        }

        /**
         * Builds {@link AttributeMap} and resets this builder.
         * @return the built object
         */
        public AttributeMap build() {
            if (entries == null) {
                return AttributeMap.EMPTY;
            }
            AttributeMap result = new AttributeMap(this);
            entries = null;
            return result;
        }
    }
}
