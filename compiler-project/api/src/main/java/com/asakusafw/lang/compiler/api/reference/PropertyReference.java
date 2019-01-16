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
package com.asakusafw.lang.compiler.api.reference;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents a property of data models.
 */
public interface PropertyReference extends Reference {

    /**
     * Returns the original declaration.
     * This typically {@code get<property-name>Option()} method.
     * @return the original declaration
     */
    MethodDescription getDeclaration();

    /**
     * Returns the declaring data model.
     * @return the declaring data model
     */
    DataModelReference getOwner();

    /**
     * Returns the property name.
     * @return the property name
     */
    PropertyName getName();

    /**
     * Returns the property type.
     * This typically a sub-type of {@code ValueOption} class.
     * @return the property type
     */
    TypeDescription getType();
}
