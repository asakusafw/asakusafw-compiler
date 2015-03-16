package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.BatchDescription;

@SuppressWarnings("javadoc")
public class BatchWithoutAnnotation extends BatchDescription {

    @Override
    protected void describe() {
        return;
    }
}
