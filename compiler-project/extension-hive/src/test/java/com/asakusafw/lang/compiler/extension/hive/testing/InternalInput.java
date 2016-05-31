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
package com.asakusafw.lang.compiler.extension.hive.testing;

import com.asakusafw.lang.compiler.internalio.InternalImporterDescription;

/**
 * An external input using internal strategy.
 * @since 0.3.1
 */
public class InternalInput extends InternalImporterDescription {

    private final Class<?> dataType;

    private final String pathPrefix;

    private DataSize dataSize;

    InternalInput(Class<?> dataType, String pathPrefix) {
        this.dataType = dataType;
        this.pathPrefix = pathPrefix;
    }

    /**
     * Returns a new instance.
     * @param dataType the data type
     * @param pathPrefix the path prefix, must be end with {@code -*}.
     * @return the created instance
     */
    public static InternalInput of(Class<?> dataType, String pathPrefix) {
        return new InternalInput(dataType, pathPrefix);
    }

    @Override
    public Class<?> getModelType() {
        return dataType;
    }

    @Override
    public String getPathPrefix() {
        return pathPrefix;
    }

    @Override
    public DataSize getDataSize() {
        if (dataSize == null) {
            return DataSize.UNKNOWN;
        }
        return dataSize;
    }

    /**
     * Sets a new value for {@link #getDataSize()}.
     * @param newValue the value to set
     * @return this
     */
    public InternalInput withDataSize(DataSize newValue) {
        this.dataSize = newValue;
        return this;
    }
}
