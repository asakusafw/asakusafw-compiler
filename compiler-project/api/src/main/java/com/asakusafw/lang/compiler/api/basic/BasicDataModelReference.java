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
package com.asakusafw.lang.compiler.api.basic;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * A basic implementation of {@link DataModelReference}.
 */
public class BasicDataModelReference extends BasicAttributeContainer implements DataModelReference {

    private final ClassDescription declaration;

    final Map<PropertyName, PropertyReference> properties = new LinkedHashMap<>();

    BasicDataModelReference(ClassDescription declaration) {
        this.declaration = declaration;
    }

    /**
     * Creates a new builder for this class.
     * @param declaration the original data model declaration
     * @return the created builder
     */
    public static Builder builder(ClassDescription declaration) {
        return new Builder(declaration);
    }

    @Override
    public ClassDescription getDeclaration() {
        return declaration;
    }

    @Override
    public Collection<PropertyReference> getProperties() {
        return properties.values();
    }

    @Override
    public PropertyReference findProperty(PropertyName name) {
        return properties.get(name);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "DataModel({0})", //$NON-NLS-1$
                declaration.getClassName());
    }

    /**
     * Builder for {@link BasicDataModelReference}.
     * @see BasicDataModelReference#builder(ClassDescription)
     */
    public static final class Builder {

        private final ClassDescription declaration;

        private final Map<PropertyName, PropertyInfo> properties = new LinkedHashMap<>();

        /**
         * Creates a new instance.
         * @param declaration the original data model declaration
         */
        public Builder(ClassDescription declaration) {
            this.declaration = declaration;
        }

        /**
         * Adds a data model property into building data model.
         * @param name the property name
         * @param type the property type
         * @param method the original property method
         * @return this
         */
        public Builder property(PropertyName name, TypeDescription type, MethodDescription method) {
            properties.put(name, new PropertyInfo(method, type));
            return this;
        }

        /**
         * Returns {@link BasicDataModelReference} with added properties.
         * @return the built instance
         */
        public BasicDataModelReference build() {
            BasicDataModelReference result = new BasicDataModelReference(declaration);
            for (Map.Entry<PropertyName, PropertyInfo> entry : properties.entrySet()) {
                PropertyName name = entry.getKey();
                PropertyInfo info = entry.getValue();
                result.properties.put(name, new BasicPropertyReference(info.declaration, result, name, info.type));
            }
            return result;
        }
    }

    private static final class PropertyInfo {

        final MethodDescription declaration;

        final TypeDescription type;

        PropertyInfo(MethodDescription declaration, TypeDescription type) {
            this.declaration = declaration;
            this.type = type;
        }
    }
}
