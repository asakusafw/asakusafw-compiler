package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

@SuppressWarnings("javadoc")
@Batch(name = "BatchWithWrongJobflow")
public class BatchWithWrongJobflow extends BatchDescription {

    @Override
    protected void describe() {
        run(MockJobflow.class).soon();
        run(JobflowWithWrongDescription.class).soon();
    }
}
