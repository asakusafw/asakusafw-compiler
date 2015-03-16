package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

@SuppressWarnings("javadoc")
@Batch(name = "BatchWithDuplicateJobflow")
public class BatchWithDuplicateJobflow extends BatchDescription {

    @Override
    protected void describe() {
        run(MockJobflow.class).soon();
        run(MockJobflow.class).soon();
    }
}
