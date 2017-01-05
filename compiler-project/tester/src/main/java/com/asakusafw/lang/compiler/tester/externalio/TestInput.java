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
package com.asakusafw.lang.compiler.tester.externalio;

import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ImmediateDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * An implementation of {@link ImporterDescription} for testing.
 * {@link TestExternalPortProcessor} can process this description.
 * @since 0.4.0
 */
public abstract class TestInput implements ImporterDescription {

    /**
     * Creates a new instance.
     * @param id the input ID
     * @param dataType the data type
     * @return the created instance
     */
    public static TestInput.Basic of(String id, Class<?> dataType) {
        return new Basic(id, dataType, DataSize.UNKNOWN);
    }

    /**
     * Creates a new instance.
     * @param id the input ID
     * @param dataType the data type
     * @param dataSize the data size
     * @return the created instance
     */
    public static TestInput.Basic of(String id, Class<?> dataType, DataSize dataSize) {
        return new Basic(id, dataType, dataSize);
    }

    /**
     * Creates a new instance.
     * @param id the input ID
     * @param dataType the data type
     * @param dataSize the data size
     * @return the created instance
     */
    public static TestInput.Basic of(String id, Class<?> dataType, ExternalInputInfo.DataSize dataSize) {
        return new Basic(id, dataType, DataSize.valueOf(dataSize.name()));
    }

    /**
     * Returns the ID.
     * @return the ID
     */
    public abstract String getId();

    /**
     * Returns an {@link ExternalInputInfo} object for this.
     * @return {@link ExternalInputInfo} object
     */
    public ExternalInputInfo toInfo() {
        return new ExternalInputInfo.Basic(
                Descriptions.classOf(getClass()),
                TestExternalPortProcessor.MODULE_NAME,
                Descriptions.classOf(getModelType()),
                ExternalInputInfo.DataSize.valueOf(getDataSize().name()),
                new ImmediateDescription(Descriptions.typeOf(String.class), getId()));
    }

    /**
     * A basic implementation of {@link TestInput}.
     * @since 0.4.0
     */
    public static class Basic extends TestInput {

        private final String id;

        private final Class<?> aClass;

        private final DataSize dataSize;

        /**
         * Creates a new instance.
         * @param id the input ID
         * @param aClass the data type
         * @param dataSize the data size
         */
        public Basic(String id, Class<?> aClass, DataSize dataSize) {
            this.id = id;
            this.aClass = aClass;
            this.dataSize = dataSize;
        }

        @Override
        public String getId() {
            return id;
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
