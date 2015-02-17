package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.Batch.Parameter;
import com.asakusafw.vocabulary.batch.BatchDescription;

@SuppressWarnings("javadoc")
@Batch(
        name = "BatchWithAttributes",
        comment = "testing",
        strict = true,
        parameters = {
                @Parameter(key = "a", comment = "A", required = false, pattern = "a+"),
                @Parameter(key = "b")
        }
)
public class BatchWithAttributes extends BatchDescription {

    @Override
    protected void describe() {
        return;
    }
}
