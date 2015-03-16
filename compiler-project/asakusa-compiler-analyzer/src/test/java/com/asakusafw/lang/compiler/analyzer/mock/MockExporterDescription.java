package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.external.ExporterDescription;

/**
 * Mock {@link ExporterDescription}.
 */
public class MockExporterDescription implements ExporterDescription {

    @Override
    public Class<?> getModelType() {
        return String.class;
    }
}
