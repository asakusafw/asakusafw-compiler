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
package com.asakusafw.lang.compiler.model.info;

import java.text.MessageFormat;
import java.util.Objects;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;

/**
 * Structural information of external inputs.
 */
public interface ExternalOutputInfo extends ExternalPortInfo {

    /**
     * A basic implementation of {@link ExternalOutputInfo}.
     * Clients can inherit this class.
     */
    public static class Basic implements ExternalOutputInfo {

        private final ClassDescription descriptionClass;

        private final ClassDescription dataModelClass;

        private final String moduleName;

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
            this(descriptionClass, moduleName, dataModelClass, null);
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
            this.descriptionClass = descriptionClass;
            this.dataModelClass = dataModelClass;
            this.moduleName = moduleName;
            this.contents = contents;
        }

        /**
         * Creates a new instance.
         * @param info the original information
         */
        public Basic(ExternalOutputInfo info) {
            this(info.getDescriptionClass(), info.getModuleName(), info.getDataModelClass(), info.getContents());
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
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + descriptionClass.hashCode();
            result = prime * result + moduleName.hashCode();
            result = prime * result + dataModelClass.hashCode();
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
