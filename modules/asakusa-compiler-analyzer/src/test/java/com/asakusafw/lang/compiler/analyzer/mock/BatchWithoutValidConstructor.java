package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

@SuppressWarnings("javadoc")
@Batch(name = "BatchWithoutValidConstructor")
public class BatchWithoutValidConstructor extends BatchDescription {

    public BatchWithoutValidConstructor(int other) {
        return;
    }

    @Override
    protected void describe() {
        // TODO Auto-generated method stub
    }
}
