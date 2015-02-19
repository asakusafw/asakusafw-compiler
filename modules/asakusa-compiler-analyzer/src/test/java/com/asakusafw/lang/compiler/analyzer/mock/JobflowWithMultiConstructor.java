package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.JobFlow;
import com.asakusafw.vocabulary.flow.Out;

@SuppressWarnings("javadoc")
@JobFlow(name = "JobflowWithMultiConstructor")
public class JobflowWithMultiConstructor extends FlowDescription {

    final In<String> in;

    final Out<String> out;

    public JobflowWithMultiConstructor(
            @Import(name = "in", description = MockImporterDescription.class) In<String> in,
            @Export(name = "out", description = MockExporterDescription.class) Out<String> out) {
        this.in = in;
        this.out = out;
    }

    public JobflowWithMultiConstructor(
            @Export(name = "out", description = MockExporterDescription.class) Out<String> out,
            @Import(name = "in", description = MockImporterDescription.class) In<String> in) {
        this.in = in;
        this.out = out;
    }

    @Override
    protected void describe() {
        return;
    }
}