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
package com.asakusafw.lang.compiler.model.info;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;

/**
 * Structural information of external inputs.
 * @since 0.1.0
 * @version 0.3.1
 */
public interface ExternalOutputInfo extends ExternalPortInfo {

    /**
     * Returns whether this output has {@link OperatorConstraint#GENERATOR generator} constraint or not.
     * @return {@code true} if this output has generator constraint, otherwise {@code false}
     */
    boolean isGenerator();

    /**
     * A basic implementation of {@link ExternalOutputInfo}.
     * Clients can inherit this class.
     * @since 0.1.0
     * @version 0.3.1
     */
    class Basic implements ExternalOutputInfo {

        private final ClassDescription descriptionClass;

        private final ClassDescription dataModelClass;

        private final String moduleName;

        private final boolean generator;

        private final Set<String> parameterNames;

        private final ValueDescription contents;

        /**
         * Creates a new instance.
         * @param descriptionClass the original exporter description class
         * @param moduleName the exporter module name
         * @param dataModelClass the target data model class
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass) {
            this(descriptionClass, moduleName, dataModelClass, Collections.emptySet(), null);
        }

        /**
         * Creates a new instance.
         * @param descriptionClass the original exporter description class
         * @param moduleName the exporter module name
         * @param dataModelClass the target data model class
         * @param contents the processor specific contents (nullable)
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass,
                ValueDescription contents) {
            this(descriptionClass, moduleName, dataModelClass, Collections.emptySet(), contents);
        }

        /**
         * Creates a new instance.
         * @param descriptionClass the original exporter description class
         * @param moduleName the exporter module name
         * @param dataModelClass the target data model class
         * @param parameterNames the required parameter names
         * @since 0.3.0
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass,
                Set<String> parameterNames) {
            this(descriptionClass, moduleName, dataModelClass, Collections.emptySet(), null);
        }

        /**
         * Creates a new instance.
         * @param descriptionClass the original exporter description class
         * @param moduleName the exporter module name
         * @param dataModelClass the target data model class
         * @param parameterNames the required parameter names
         * @param contents the processor specific contents (nullable)
         * @since 0.3.0
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass,
                Set<String> parameterNames,
                ValueDescription contents) {
            this(descriptionClass, moduleName, dataModelClass, false, parameterNames, contents);
        }

        /**
         * Creates a new instance.
         * @param descriptionClass the original exporter description class
         * @param moduleName the exporter module name
         * @param dataModelClass the target data model class
         * @param generator {@code true} if this output has generator constraint, otherwise {@code false}
         * @param parameterNames the required parameter names
         * @since 0.3.1
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass,
                boolean generator,
                Set<String> parameterNames) {
            this(descriptionClass, moduleName, dataModelClass, generator, Collections.emptySet(), null);
        }

        /**
         * Creates a new instance.
         * @param descriptionClass the original exporter description class
         * @param moduleName the exporter module name
         * @param dataModelClass the target data model class
         * @param generator {@code true} if this output has generator constraint, otherwise {@code false}
         * @param parameterNames the required parameter names
         * @param contents the processor specific contents (nullable)
         * @since 0.3.1
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass,
                boolean generator,
                Set<String> parameterNames,
                ValueDescription contents) {
            this.descriptionClass = descriptionClass;
            this.dataModelClass = dataModelClass;
            this.moduleName = moduleName;
            this.generator = generator;
            this.parameterNames = Collections.unmodifiableSet(new LinkedHashSet<>(parameterNames));
            this.contents = contents;
        }

        /**
         * Creates a new instance.
         * @param info the original information
         */
        public Basic(ExternalOutputInfo info) {
            this(info.getDescriptionClass(), info.getModuleName(), info.getDataModelClass(),
                    info.getParameterNames(), info.getContents());
        }

        @Override
        public ClassDescription getDescriptionClass() {
            return descriptionClass;
        }

        @Override
        public ClassDescription getDataModelClass() {
            return dataModelClass;
        }

        @Override
        public String getModuleName() {
            return moduleName;
        }

        @Override
        public boolean isGenerator() {
            return generator;
        }

        @Override
        public Set<String> getParameterNames() {
            return parameterNames;
        }

        @Override
        public ValueDescription getContents() {
            return contents;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + descriptionClass.hashCode();
            result = prime * result + moduleName.hashCode();
            result = prime * result + dataModelClass.hashCode();
            result = prime * result + (generator ? 1 : 0);
            result = prime * result + parameterNames.hashCode();
            result = prime * result + Objects.hashCode(contents);
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
            Basic other = (Basic) obj;
            if (!descriptionClass.equals(other.descriptionClass)) {
                return false;
            }
            if (!moduleName.equals(other.moduleName)) {
                return false;
            }
            if (!dataModelClass.equals(other.dataModelClass)) {
                return false;
            }
            if (generator != other.generator) {
                return false;
            }
            if (!parameterNames.equals(other.parameterNames)) {
                return false;
            }
            if (!Objects.equals(contents, other.contents)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "ExternalOutput(module={0}, description={1})", //$NON-NLS-1$
                    moduleName,
                    descriptionClass);
        }
    }
}
