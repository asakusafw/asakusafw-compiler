package com.asakusafw.lang.compiler.api.mock;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.basic.AbstractExternalPortProcessorContext;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Mock implementation of {@link ExternalPortProcessor}.
 */
public class MockExternalPortProcessor implements ExternalPortProcessor {

    private static final String MODULE_NAME = "mock";

    /**
     * A dummy context for this processor.
     */
    public static final Context CONTEXT = new AbstractExternalPortProcessorContext() {
        @Override
        public CompilerOptions getOptions() {
            throw new UnsupportedOperationException();
        }
        @Override
        public DataModelLoader getDataModelLoader() {
            throw new UnsupportedOperationException();
        }
        @Override
        public ClassLoader getClassLoader() {
            return MockExternalPortProcessor.class.getClassLoader();
        }
        @Override
        public OutputStream addResourceFile(Location location) throws IOException {
            throw new UnsupportedOperationException();
        }
    };

    @Override
    public boolean isSupported(AnalyzeContext context, Class<?> descriptionClass) {
        return ImporterDescription.class.isAssignableFrom(descriptionClass)
                || ExporterDescription.class.isAssignableFrom(descriptionClass);
    }

    @Override
    public ExternalInputInfo analyzeInput(AnalyzeContext context, String name, Object description) {
        if ((description instanceof ImporterDescription) == false) {
            throw new IllegalArgumentException();
        }
        ImporterDescription desc = (ImporterDescription) description;
        return new ExternalInputInfo.Basic(
                Descriptions.classOf(desc.getClass()),
                MODULE_NAME,
                Descriptions.classOf(desc.getModelType()),
                ExternalInputInfo.DataSize.valueOf(desc.getDataSize().name()));
    }

    @Override
    public ExternalOutputInfo analyzeOutput(AnalyzeContext context, String name, Object description) {
        if ((description instanceof ExporterDescription) == false) {
            throw new IllegalArgumentException();
        }
        ExporterDescription desc = (ExporterDescription) description;
        return new ExternalOutputInfo.Basic(
                Descriptions.classOf(desc.getClass()),
                MODULE_NAME,
                Descriptions.classOf(desc.getModelType()));
    }

    @Override
    public ExternalInputReference resolveInput(Context context, String name, ExternalInputInfo info) {
        String relativePath = String.format("%s/input/%s-*", MODULE_NAME, name);
        String path = context.getOptions().getRuntimeWorkingPath(relativePath);
        return new ExternalInputReference(name, info, Collections.singleton(path));
    }

    @Override
    public ExternalOutputReference resolveOutput(
            Context context,
            String name,
            ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        return new ExternalOutputReference(name, info, internalOutputPaths);
    }

    @Override
    public void process(Context context,
            List<ExternalInputReference> inputs,
            List<ExternalOutputReference> outputs) throws IOException {
        return;
    }
}
