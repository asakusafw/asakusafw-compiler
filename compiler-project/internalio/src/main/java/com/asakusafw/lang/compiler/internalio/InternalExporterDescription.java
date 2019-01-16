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

import com.asakusafw.vocabulary.external.ExporterDescription;

/**
 * An {@link ExporterDescription} for exporting data using the internal format.
 * @since 0.3.0
 */
public abstract class InternalExporterDescription implements ExporterDescription {

    /**
     * Returns the export target path prefix.
     * <p>
     * Each path segment must be separated by {@code "/"}.
     * The file name (the last segment of the path prefix) must consist of digits and alphabets,
     * and must end with {@code "-*"}.
     * </p>
     * @return the export target path prefix
     */
    public abstract String getPathPrefix();

    @Override
    public String toString() {
        return MessageFormat.format(
                "InternalExporter({0})", //$NON-NLS-1$
                getPathPrefix());
    }

    /**
     * Basic implementation of {@link InternalExporterDescription}.
     */
    public static class Basic extends InternalExporterDescription {

        private final Class<?> modelType;

        private final String pathPrefix;

        /**
         * Creates a new instance.
         * @param modelType the data type
         * @param pathPrefix the path prefix
         */
        public Basic(Class<?> modelType, String pathPrefix) {
            this.modelType = modelType;
            this.pathPrefix = pathPrefix;
        }

        @Override
        public Class<?> getModelType() {
            return modelType;
        }

        @Override
        public String getPathPrefix() {
            return pathPrefix;
        }
    }
}
