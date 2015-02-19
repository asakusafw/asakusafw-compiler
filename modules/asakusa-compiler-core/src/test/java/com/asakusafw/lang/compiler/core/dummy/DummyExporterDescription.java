package com.asakusafw.lang.compiler.core.dummy;

import com.asakusafw.vocabulary.external.ExporterDescription;

/**
 * Mock implementation of {@link ExporterDescription}.
 */
public class DummyExporterDescription implements ExporterDescription {

    @Override
    public Class<?> getModelType() {
        return String.class;
    }
}
