package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

@SuppressWarnings("javadoc")
@Batch(name = "$WRONG")
public class BatchWithWrongName extends BatchDescription {

    @Override
    protected void describe() {
        return;
    }
}
