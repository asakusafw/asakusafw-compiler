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
package com.asakusafw.lang.compiler.api.basic;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * A basic implementation of {@link PropertyReference}.
 */
public class BasicPropertyReference extends BasicAttributeContainer implements PropertyReference {

    private final MethodDescription declaration;

    private final DataModelReference owner;

    private final PropertyName name;

    private final TypeDescription type;

    /**
     * Creates a new instance.
     * @param declaration the original declaration
     * @param owner the owner of this property
     * @param name the property name
     * @param type the property type
     */
    public BasicPropertyReference(
            MethodDescription declaration,
            DataModelReference owner,
            PropertyName name,
            TypeDescription type) {
        this.declaration = declaration;
        this.owner = owner;
        this.name = name;
        this.type = type;
    }

    @Override
    public MethodDescription getDeclaration() {
        return declaration;
    }

    @Override
    public DataModelReference getOwner() {
        return owner;
    }

    @Override
    public PropertyName getName() {
        return name;
    }

    @Override
    public TypeDescription getType() {
        return type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + name.hashCode();
        result = prime * result + type.hashCode();
        result = prime * result + declaration.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BasicPropertyReference other = (BasicPropertyReference) obj;
        if (!name.equals(other.name)) {
            return false;
        }
        if (!type.equals(other.type)) {
            return false;
        }
        if (!declaration.equals(other.declaration)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Property({0}#{1}:{2})", //$NON-NLS-1$
                declaration.getDeclaringClass().getSimpleName(),
                name,
                type);
    }
}
