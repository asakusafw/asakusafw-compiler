package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.JobFlow;
import com.asakusafw.vocabulary.flow.Out;

@SuppressWarnings("javadoc")
@JobFlow(name = "JobflowWithInconsistentInput")
public class JobflowWithInconsistentInput extends FlowDescription {

    public JobflowWithInconsistentInput(
            @Import(name = "in", description = MockImporterDescription.class) In<Integer> in,
            @Export(name = "out", description = MockExporterDescription.class) Out<String> out) {
        return;
    }

    @Override
    protected void describe() {
        return;
    }
}
