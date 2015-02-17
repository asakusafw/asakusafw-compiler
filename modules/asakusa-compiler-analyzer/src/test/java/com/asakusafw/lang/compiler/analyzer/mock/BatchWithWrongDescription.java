package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

@SuppressWarnings("javadoc")
@Batch(name = "BatchWithWrongDescription")
public class BatchWithWrongDescription extends BatchDescription {

    @Override
    protected void describe() {
        throw new UnsupportedOperationException();
    }
}
