package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

/**
 * Abstract batch.
 */
@Batch(name = "AbstractBatch")
public abstract class AbstractBatch extends BatchDescription {

    @Override
    protected void describe() {
        return;
    }
}
