package com.asakusafw.lang.compiler.tester.executor.util;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

/**
 * A dummy batch class.
 */
@Batch(name = DummyBatchClass.ID)
public class DummyBatchClass extends BatchDescription {

    /**
     * The batch ID for this class.
     */
    public static final String ID = "dummy"; //$NON-NLS-1$

    @Override
    protected void describe() {
        throw new UnsupportedOperationException();
    }
}
