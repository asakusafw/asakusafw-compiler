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
package com.asakusafw.lang.compiler.internalio;

import java.text.MessageFormat;

import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * An {@link ImporterDescription} for importing data using the internal format.
 * @since 0.3.0
 */
public abstract class InternalImporterDescription implements ImporterDescription {

    /**
     * Returns the import target path prefix.
     * <p>
     * Each path segment must be separated by {@code "/"}.
     * And the file name (the last segment of the path prefix) must end with {@code "-*"}.
     * </p>
     * @return the import target paths
     */
    public abstract String getPathPrefix();

    @Override
    public String toString() {
        return MessageFormat.format(
                "InternalImporter({0})", //$NON-NLS-1$
                getPathPrefix());
    }

    /**
     * Basic implementation of {@link InternalImporterDescription}.
     */
    public static class Basic extends InternalImporterDescription {

        private final Class<?> modelType;

        private final String pathPrefix;

        private final DataSize dataSize;

        /**
         * Creates a new instance.
         * @param modelType the data type
         * @param pathPrefix the path prefix
         */
        public Basic(Class<?> modelType, String pathPrefix) {
            this(modelType, pathPrefix, DataSize.UNKNOWN);
        }

        /**
         * Creates a new instance.
         * @param modelType the data type
         * @param pathPrefix the path prefix
         * @param dataSize the data size
         */
        public Basic(Class<?> modelType, String pathPrefix, DataSize dataSize) {
            this.modelType = modelType;
            this.pathPrefix = pathPrefix;
            this.dataSize = dataSize;
        }

        @Override
        public Class<?> getModelType() {
            return modelType;
        }

        @Override
        public String getPathPrefix() {
            return pathPrefix;
        }

        @Override
        public DataSize getDataSize() {
            return dataSize;
        }
    }
}
