package com.asakusafw.lang.compiler.analyzer.mock;

import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.Out;

@SuppressWarnings("javadoc")
public class JobflowWithoutAnnotation extends FlowDescription {

    final In<String> in;

    final Out<String> out;

    public JobflowWithoutAnnotation(
            @Import(name = "in", description = MockImporterDescription.class) In<String> in,
            @Export(name = "out", description = MockExporterDescription.class) Out<String> out) {
        this.in = in;
        this.out = out;
    }

    @Override
    protected void describe() {
        return;
    }
}
