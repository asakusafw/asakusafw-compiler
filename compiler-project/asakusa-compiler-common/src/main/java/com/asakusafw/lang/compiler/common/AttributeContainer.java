/**
 * Copyright 2011-2015 Asakusa Framework Team.
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

/**
 * An abstract super interface which provides free-formed attributes.
 */
public interface AttributeContainer {

    /**
     * Returns a registered attribute.
     * @param type the attribute type
     * @param <T> the attribute type
     * @return the corresponded attribute, or {@code null} if such an attribute is not registered
     */
    <T> T getAttribute(Class<T> type);

    /**
     * Registers an attribute.
     * @param type the attribute type
     * @param value the attribute value, or {@code null} to remove the registered attribute
     * @param <T> the attribute type
     */
    <T> void putAttribute(Class<T> type, T value);

    /**
     * Returns the registered attribute types.
     * @return the registered attribute types
     */
    Collection<Class<?>> getAttributeTypes();
}
