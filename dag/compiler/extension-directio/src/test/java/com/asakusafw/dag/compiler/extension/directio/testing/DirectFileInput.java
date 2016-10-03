/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.extension.directio.testing;

import com.asakusafw.runtime.directio.DataFilter;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.vocabulary.directio.DirectFileInputDescription;

/**
 * Mock implementation of {@link DirectFileInputDescription}.
 */
public class DirectFileInput extends DirectFileInputDescription {

    private final Class<?> modelType;

    private final String basePath;

    private final String resourcePattern;

    private final Class<? extends DataFormat<?>> format;

    private Class<? extends DataFilter<?>> filter;

    private boolean optional;

    private DataSize dataSize;

    DirectFileInput(
            Class<?> modelType,
            String basePath, String resourcePattern,
            Class<? extends DataFormat<?>> format) {
        this.modelType = modelType;
        this.basePath = basePath;
        this.resourcePattern = resourcePattern;
        this.format = format;
    }

    /**
     * Creates a new instance.
     * @param dataType the data type
     * @param basePath the base path
     * @param pattern the resource pattern
     * @param dataFormat the data format type
     * @return the created instance
     */
    public static DirectFileInput of(
            Class<?> dataType,
            String basePath, String pattern,
            Class<? extends DataFormat<?>> dataFormat) {
        return new DirectFileInput(dataType, basePath, pattern, dataFormat);
    }

    /**
     * Creates a new instance.
     * @param basePath the base path
     * @param pattern the resource pattern
     * @param dataFormat the data format type
     * @return the created instance
     */
    public static DirectFileInput of(
            String basePath, String pattern,
            Class<? extends DataFormat<?>> dataFormat) {
        try {
            Class<?> dataType = dataFormat.newInstance().getSupportedType();
            return of(dataType, basePath, pattern, dataFormat);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public Class<?> getModelType() {
        return modelType;
    }

    @Override
    public String getBasePath() {
        return basePath;
    }

    @Override
    public String getResourcePattern() {
        return resourcePattern;
    }

    @Override
    public Class<? extends DataFormat<?>> getFormat() {
        return format;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public Class<? extends DataFilter<?>> getFilter() {
        return filter;
    }

    @Override
    public DataSize getDataSize() {
        return dataSize;
    }

    /**
     * Sets a new value for {@link #getFilter()}.
     * @param newValue the value to set
     * @return this
     */
    public DirectFileInput withFilter(Class<? extends DataFilter<?>> newValue) {
        this.filter = newValue;
        return this;
    }

    /**
     * Sets a new value for {@link #isOptional()}.
     * @param newValue the value to set
     * @return this
     */
    public DirectFileInput withOptional(boolean newValue) {
        this.optional = newValue;
        return this;
    }

    /**
     * Sets a new value for {@link #getDataSize()}.
     * @param newValue the value to set
     * @return this
     */
    public DirectFileInput withDataSize(DataSize newValue) {
        this.dataSize = newValue;
        return this;
    }
}
