package com.asakusafw.lang.compiler.cli.mock;

import java.io.IOException;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.model.graph.Jobflow;

/**
 * Mock {@link JobflowProcessor}.
 */
@SuppressWarnings("javadoc")
public class DummyJobflowProcessor implements JobflowProcessor, DummyElement {

    final String id;

    public DummyJobflowProcessor() {
        this("default");
    }

    public DummyJobflowProcessor(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void process(Context context, Jobflow source) throws IOException {
        return;
    }
}
