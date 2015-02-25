package com.asakusafw.lang.compiler.cli.mock;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Mock {@link ExternalPortProcessor}.
 */
@SuppressWarnings("javadoc")
public class DummyExternalPortProcessor implements ExternalPortProcessor, DummyElement {

    final String id;

    public DummyExternalPortProcessor() {
        this("default");
    }

    public DummyExternalPortProcessor(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isSupported(AnalyzeContext context, Class<?> descriptionClass) {
        return false;
    }

    @Override
    public ExternalInputInfo analyzeInput(AnalyzeContext context, String name, Object description) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExternalOutputInfo analyzeOutput(AnalyzeContext context, String name, Object description) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExternalInputReference resolveInput(Context context, String name, ExternalInputInfo info) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExternalOutputReference resolveOutput(Context context, String name, ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void process(Context context, List<ExternalInputReference> inputs, List<ExternalOutputReference> outputs)
            throws IOException {
        throw new UnsupportedOperationException();
    }
}
