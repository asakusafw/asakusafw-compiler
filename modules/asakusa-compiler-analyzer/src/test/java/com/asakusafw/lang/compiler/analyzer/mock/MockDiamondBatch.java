package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;
import com.asakusafw.vocabulary.batch.Work;

@SuppressWarnings("javadoc")
@Batch(name = "MockDiamondBatch")
public class MockDiamondBatch extends BatchDescription {

    @Override
    protected void describe() {
        /*
         * a +-- b --+ d
         *    \- c -/
         */
        Work a = run(MockJobA.class).soon();
        Work b = run(MockJobB.class).after(a);
        Work c = run(MockJobC.class).after(a);
        run(MockJobD.class).after(b, c);
    }
}
