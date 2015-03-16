package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Mock {@link ImporterDescription}.
 */
public class InvalidImporterDescription implements ImporterDescription {

    InvalidImporterDescription() {
        return;
    }

    @Override
    public Class<?> getModelType() {
        return String.class;
    }

    @Override
    public DataSize getDataSize() {
        return DataSize.UNKNOWN;
    }
}
