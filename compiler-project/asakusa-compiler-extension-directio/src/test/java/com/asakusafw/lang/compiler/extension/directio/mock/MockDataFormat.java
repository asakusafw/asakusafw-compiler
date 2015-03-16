package com.asakusafw.lang.compiler.extension.directio.mock;

import com.asakusafw.runtime.directio.DataFormat;

/**
 * Mock {@link DataFormat}.
 */
public class MockDataFormat extends WritableDataFormat<MockData> {

    @Override
    public Class<MockData> getSupportedType() {
        return MockData.class;
    }
}
