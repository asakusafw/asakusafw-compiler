package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.JobFlow;
import com.asakusafw.vocabulary.flow.Out;

@SuppressWarnings("javadoc")
@JobFlow(name = "JobflowWithInconsistentOutput")
public class JobflowWithInconsistentOutput extends FlowDescription {

    public JobflowWithInconsistentOutput(
            @Import(name = "in", description = MockImporterDescription.class) In<String> in,
            @Export(name = "out", description = MockExporterDescription.class) Out<Integer> out) {
        return;
    }

    @Override
    protected void describe() {
        return;
    }
}
