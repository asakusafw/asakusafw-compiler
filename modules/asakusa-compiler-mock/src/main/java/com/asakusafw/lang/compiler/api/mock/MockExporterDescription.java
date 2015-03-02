package com.asakusafw.lang.compiler.api.mock;

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
                "dummy",
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
