package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

@SuppressWarnings("javadoc")
@Batch(name = "BatchWithWrongConstructor")
public class BatchWithWrongConstructor extends BatchDescription {

    public BatchWithWrongConstructor() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void describe() {
        return;
    }
}
