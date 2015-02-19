package com.asakusafw.lang.compiler.core.dummy;

import java.io.IOException;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.reference.BatchReference;

/**
 * Mock {@link BatchProcessor}.
 */
@SuppressWarnings("javadoc")
public class DummyBatchProcessor implements BatchProcessor, DummyElement {

    final String id;

    public DummyBatchProcessor() {
        this("default");
    }

    public DummyBatchProcessor(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void process(Context context, BatchReference source) throws IOException {
        return;
    }
}
