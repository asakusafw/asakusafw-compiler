package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.JobFlow;
import com.asakusafw.vocabulary.flow.Out;

@SuppressWarnings("javadoc")
@JobFlow(name = "JobflowWithWrongConstructor")
public class JobflowWithWrongConstructor extends FlowDescription {

    public JobflowWithWrongConstructor(
            @Import(name = "in", description = MockImporterDescription.class) In<String> in,
            @Export(name = "out", description = MockExporterDescription.class) Out<String> out) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void describe() {
        return;
    }
}
