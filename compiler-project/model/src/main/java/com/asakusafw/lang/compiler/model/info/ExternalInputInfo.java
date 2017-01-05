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
package com.asakusafw.lang.compiler.model.info;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;

/**
 * Structural information of external inputs.
 */
public interface ExternalInputInfo extends ExternalPortInfo {

    /**
     * Returns the estimated data size.
     * @return the estimated data size
     */
    DataSize getDataSize();

    /**
     * Represents a kind of estimated input data size.
     */
    enum DataSize {

        /**
         * unknown or not estimated.
         */
        UNKNOWN,

        /**
         * tiny data (~10MB).
         */
        TINY,

        /**
         * small data (~200MB).
         */
        SMALL,

        /**
         * large data (200MB~).
         */
        LARGE,
    }

    /**
     * A basic implementation of {@link ExternalInputInfo}.
     * Clients can inherit this class.
     * @since 0.1.0
     * @version 0.3.0
     */
    class Basic implements ExternalInputInfo {

        private final ClassDescription descriptionClass;

        private final ClassDescription dataModelClass;

        private final String moduleName;

        private final ValueDescription contents;

        private final DataSize dataSize;

        private final Set<String> parameterNames;

        /**
         * Creates a new instance.
         * @param descriptionClass the original importer description class
         * @param dataModelClass the target data model class
         * @param moduleName the importer module name
         * @param dataSize the estimated data size
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass,
                DataSize dataSize) {
            this(descriptionClass, moduleName, dataModelClass, dataSize, Collections.emptySet(), null);
        }

        /**
         * Creates a new instance.
         * @param descriptionClass the original importer description class
         * @param dataModelClass the target data model class
         * @param moduleName the importer module name
         * @param dataSize the estimated data size
         * @param contents the processor specific contents (nullable)
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass,
                DataSize dataSize,
                ValueDescription contents) {
            this(descriptionClass, moduleName, dataModelClass, dataSize, Collections.emptySet(), contents);
        }

        /**
         * Creates a new instance.
         * @param descriptionClass the original importer description class
         * @param dataModelClass the target data model class
         * @param moduleName the importer module name
         * @param dataSize the estimated data size
         * @param parameterNames the required parameter names
         * @since 0.3.0
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass,
                DataSize dataSize,
                Set<String> parameterNames) {
            this(descriptionClass, moduleName, dataModelClass, dataSize, parameterNames, null);
        }

        /**
         * Creates a new instance.
         * @param descriptionClass the original importer description class
         * @param dataModelClass the target data model class
         * @param moduleName the importer module name
         * @param dataSize the estimated data size
         * @param parameterNames the required parameter names
         * @param contents the processor specific contents (nullable)
         * @since 0.3.0
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass,
                DataSize dataSize,
                Set<String> parameterNames,
                ValueDescription contents) {
            this.descriptionClass = descriptionClass;
            this.dataModelClass = dataModelClass;
            this.moduleName = moduleName;
            this.dataSize = dataSize;
            this.parameterNames = Collections.unmodifiableSet(new LinkedHashSet<>(parameterNames));
            this.contents = contents;
        }

        /**
         * Creates a new instance.
         * @param info the original information
         */
        public Basic(ExternalInputInfo info) {
            this(info.getDescriptionClass(), info.getModuleName(), info.getDataModelClass(),
                    info.getDataSize(), info.getParameterNames(), info.getContents());
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
        public ValueDescription getContents() {
            return contents;
        }

        @Override
        public DataSize getDataSize() {
            return dataSize;
        }

        @Override
        public Set<String> getParameterNames() {
            return parameterNames;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "ExternalInput(module={0}, description={1})", //$NON-NLS-1$
                    moduleName,
                    descriptionClass);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + descriptionClass.hashCode();
            result = prime * result + moduleName.hashCode();
            result = prime * result + dataModelClass.hashCode();
            result = prime * result + dataSize.hashCode();
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
            if (dataSize != other.dataSize) {
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
    }
}
