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
package com.asakusafw.dag.runtime.directio;

import com.asakusafw.dag.api.common.ObjectFactory;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.directio.DataDefinition;
import com.asakusafw.runtime.directio.DataFilter;
import com.asakusafw.runtime.directio.DataFormat;

/**
 * A basic implementation of {@link DataDefinition}.
 * @param <T> the data model type
 * @since 0.4.0
 */
public final class BasicDataDefinition<T> implements DataDefinition<T> {

    private final DataFormat<T> dataFormat;

    private final DataFilter<? super T> dataFilter;

    private BasicDataDefinition(DataFormat<T> dataFormat, DataFilter<? super T> dataFilter) {
        this.dataFormat = dataFormat;
        this.dataFilter = dataFilter;
    }

    /**
     * Creates a new instance.
     * @param dataFormat the data format
     * @param <T> the data type
     * @return the created instance
     */
    public static <T> DataDefinition<T> newInstance(DataFormat<T> dataFormat) {
        Arguments.requireNonNull(dataFormat);
        return newInstance(dataFormat, null);
    }

    /**
     * Creates a new instance.
     * @param dataFormat the data format
     * @param dataFilter the data filter (nullable)
     * @param <T> the data type
     * @return the created instance
     */
    @SuppressWarnings("unchecked")
    public static <T> DataDefinition<T> newInstance(DataFormat<T> dataFormat, DataFilter<?> dataFilter) {
        Arguments.requireNonNull(dataFormat);
        return new BasicDataDefinition<>(dataFormat, (DataFilter<? super T>) dataFilter);
    }

    /**
     * Creates a new instance.
     * @param factory the object factory
     * @param dataFormatClass the data format class
     * @param dataFilterClass the data filter class (nullable)
     * @return the created instance
     */
    public static DataDefinition<?> newInstance(
            ObjectFactory factory,
            Class<? extends DataFormat<?>> dataFormatClass,
            Class<? extends DataFilter<?>> dataFilterClass) {
        Arguments.requireNonNull(dataFormatClass);
        DataFormat<?> dataFormat = factory.newInstance(dataFormatClass);
        DataFilter<?> dataFilter = dataFilterClass == null ? null : factory.newInstance(dataFilterClass);
        return newInstance(dataFormat, dataFilter);
    }

    @Override
    public Class<? extends T> getDataClass() {
        return dataFormat.getSupportedType();
    }

    @Override
    public DataFormat<T> getDataFormat() {
        return dataFormat;
    }

    @Override
    public DataFilter<? super T> getDataFilter() {
        return dataFilter;
    }
}
