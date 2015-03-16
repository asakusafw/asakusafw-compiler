package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

@Batch(name = "NotPublicBatch")
class NotPublicBatch extends BatchDescription {

    @Override
    protected void describe() {
        return;
    }
}
