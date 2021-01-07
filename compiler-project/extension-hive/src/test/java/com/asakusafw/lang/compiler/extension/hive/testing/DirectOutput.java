/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.hive.testing;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.vocabulary.directio.DirectFileOutputDescription;

/**
 * A mock implementation of {@link DirectFileOutputDescription}.
 * @since 0.3.1
 */
public class DirectOutput extends DirectFileOutputDescription {

    private final Class<?> modelType;

    private final String basePath;

    private final String resourcePattern;

    private final Class<? extends DataFormat<?>> format;

    private List<String> deletePatterns = Collections.emptyList();

    private List<String> order = Collections.emptyList();

    DirectOutput(
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
    public static DirectOutput of(
            Class<?> dataType,
            String basePath, String pattern,
            Class<? extends DataFormat<?>> dataFormat) {
        return new DirectOutput(dataType, basePath, pattern, dataFormat);
    }

    /**
     * Creates a new instance.
     * @param basePath the base path
     * @param pattern the resource pattern
     * @param dataFormat the data format type
     * @return the created instance
     */
    public static DirectOutput of(
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
    public List<String> getOrder() {
        return order;
    }

    @Override
    public List<String> getDeletePatterns() {
        return deletePatterns;
    }

    @Override
    public Class<? extends DataFormat<?>> getFormat() {
        return format;
    }

    /**
     * Sets a new value for {@link #getOrder()}.
     * @param newValues the values to set
     * @return this
     */
    public DirectOutput withOrder(String... newValues) {
        this.order = Arrays.asList(newValues);
        return this;
    }

    /**
     * Sets a new value for {@link #getDeletePatterns()}.
     * @param newValues the values to set
     * @return this
     */
    public DirectOutput withDeletePatterns(String... newValues) {
        this.deletePatterns = Arrays.asList(newValues);
        return this;
    }
}
