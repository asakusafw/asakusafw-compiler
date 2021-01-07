/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

/**
 * Represents an entry of typed attribute.
 * @param <T> the attribute type
 * @since 0.4.1
 */
public class AttributeEntry<T> {

    private final Class<T> type;

    private final T value;

    /**
     * Creates a new instance.
     * @param type the attribute type
     * @param value the attribute value
     */
    public AttributeEntry(Class<T> type, T value) {
        this.type = type;
        this.value = value;
    }

    /**
     * Creates a new instance.
     * @param <T> the attribute type
     * @param type the attribute type
     * @param value the attribute value
     * @return the entry
     * @throws ClassCastException if the given value is not instance of the given type
     */
    public static <T> AttributeEntry<T> of(Class<T> type, Object value) {
        return new AttributeEntry<>(type, type.cast(value));
    }

    /**
     * Returns the attribute type.
     * @return the attribute type
     */
    public Class<T> getType() {
        return type;
    }

    /**
     * Returns the attribute value.
     * @return the attribute value
     */
    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
