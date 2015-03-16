package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

@SuppressWarnings("javadoc")
@Batch(name = "mock")
public class MockBatch extends BatchDescription {

    @Override
    protected void describe() {
        run(MockJobflow.class).soon();
    }
}
