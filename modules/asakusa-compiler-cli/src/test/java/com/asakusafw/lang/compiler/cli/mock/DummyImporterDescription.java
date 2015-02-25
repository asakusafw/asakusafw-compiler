package com.asakusafw.lang.compiler.cli.mock;

import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Mock implementation of {@link ImporterDescription}.
 */
public class DummyImporterDescription implements ImporterDescription {

    @Override
    public Class<?> getModelType() {
        return String.class;
    }

    @Override
    public DataSize getDataSize() {
        return DataSize.UNKNOWN;
    }
}
