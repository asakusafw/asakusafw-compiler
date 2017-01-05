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
package com.asakusafw.lang.compiler.api.testing;

import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;

/**
 * A mock implementation for {@link ExporterDescription}.
 * {@link MockExternalPortProcessor} can process this description.
 */
public abstract class MockExporterDescription implements ExporterDescription {

    /**
     * Creates a new instance.
     * @param dataType the data type
     * @return the created instance
     */
    public static MockExporterDescription newInstance(Class<?> dataType) {
        return new Basic(dataType);
    }

    /**
     * Returns an {@link ExternalOutputInfo} object for this.
     * @return {@link ExternalOutputInfo} object
     */
    public ExternalOutputInfo toInfo() {
        return new ExternalOutputInfo.Basic(
                Descriptions.classOf(getClass()),
                "dummy", //$NON-NLS-1$
                Descriptions.classOf(getModelType()));
    }

    /**
     * A basic implementation of {@link MockExporterDescription}.
     */
    public static class Basic extends MockExporterDescription {

        private final Class<?> aClass;

        /**
         * Creates a new instance.
         * @param aClass the data type
         */
        public Basic(Class<?> aClass) {
            this.aClass = aClass;
        }

        @Override
        public Class<?> getModelType() {
            return aClass;
        }
    }
}
