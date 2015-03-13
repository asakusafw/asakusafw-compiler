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
