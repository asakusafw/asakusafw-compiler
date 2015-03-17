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
package com.asakusafw.lang.compiler.api.testing;

import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * A mock implementation for {@link ImporterDescription}.
 * {@link MockExternalPortProcessor} can process this description.
 */
public abstract class MockImporterDescription implements ImporterDescription {

    /**
     * Creates a new instance.
     * @param dataType the data type
     * @return the created instance
     */
    public static MockImporterDescription newInstance(Class<?> dataType) {
        return new Basic(dataType, DataSize.UNKNOWN);
    }

    /**
     * Creates a new instance.
     * @param dataType the data type
     * @param dataSize the data size
     * @return the created instance
     */
    public static MockImporterDescription newInstance(Class<?> dataType, DataSize dataSize) {
        return new Basic(dataType, dataSize);
    }

    /**
     * Creates a new instance.
     * @param dataType the data type
     * @param dataSize the data size
     * @return the created instance
     */
    public static MockImporterDescription newInstance(Class<?> dataType, ExternalInputInfo.DataSize dataSize) {
        return new Basic(dataType, DataSize.valueOf(dataSize.name()));
    }

    @Override
    public DataSize getDataSize() {
        return DataSize.UNKNOWN;
    }

    /**
     * Returns an {@link ExternalInputInfo} object for this.
     * @return {@link ExternalInputInfo} object
     */
    public ExternalInputInfo toInfo() {
        return new ExternalInputInfo.Basic(
                Descriptions.classOf(getClass()),
                "dummy", //$NON-NLS-1$
                Descriptions.classOf(getModelType()),
                ExternalInputInfo.DataSize.valueOf(getDataSize().name()));
    }

    /**
     * A basic implementation of {@link MockImporterDescription}.
     */
    public static class Basic extends MockImporterDescription {

        private final Class<?> aClass;

        private final DataSize dataSize;

        /**
         * Creates a new instance.
         * @param aClass the data type
         * @param dataSize the data size
         */
        public Basic(Class<?> aClass, DataSize dataSize) {
            this.aClass = aClass;
            this.dataSize = dataSize;
        }

        @Override
        public Class<?> getModelType() {
            return aClass;
        }

        @Override
        public DataSize getDataSize() {
            return dataSize;
        }
    }
}
