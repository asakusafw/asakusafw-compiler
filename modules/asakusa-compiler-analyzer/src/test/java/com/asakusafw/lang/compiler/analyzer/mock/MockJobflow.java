package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.JobFlow;
import com.asakusafw.vocabulary.flow.Out;
import com.asakusafw.vocabulary.flow.util.CoreOperatorFactory.Checkpoint;
import com.asakusafw.vocabulary.flow.util.CoreOperators;

@SuppressWarnings("javadoc")
@JobFlow(name = "mock")
public class MockJobflow extends FlowDescription {

    final In<String> in;

    final Out<String> out;

    public MockJobflow(
            @Import(name = "in", description = MockImporterDescription.class) In<String> in,
            @Export(name = "out", description = MockExporterDescription.class) Out<String> out) {
        this.in = in;
        this.out = out;
    }

    @Override
    protected void describe() {
        Checkpoint<String> cp = CoreOperators.checkpoint(in);
        out.add(cp.out);
    }
}
