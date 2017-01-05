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
package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Represents a data model type.
 */
public interface DataModelReference extends Reference {

    /**
     * Returns the original declaration.
     * @return the original declaration
     */
    ClassDescription getDeclaration();

    /**
     * Returns the properties in this data model.
     * @return the properties
     */
    Collection<? extends PropertyReference> getProperties();

    /**
     * Returns a property.
     * @param name the target property name
     * @return the property, or {@code null} if it is not defined in this data model
     */
    PropertyReference findProperty(PropertyName name);
}
