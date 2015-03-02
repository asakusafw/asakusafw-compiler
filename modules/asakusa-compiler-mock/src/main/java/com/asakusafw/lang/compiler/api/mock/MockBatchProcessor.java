package com.asakusafw.lang.compiler.api.mock;

import java.io.IOException;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.reference.BatchReference;

/**
 * A mock implementation of {@link BatchProcessor}.
 * <p>
 * This processor does nothing.
 * </p>
 */
public class MockBatchProcessor implements BatchProcessor {

    @Override
    public void process(Context context, BatchReference source) throws IOException {
        return;
    }
}
