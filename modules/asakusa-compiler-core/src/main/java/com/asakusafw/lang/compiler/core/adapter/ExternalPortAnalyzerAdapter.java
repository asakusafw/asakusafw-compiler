package com.asakusafw.lang.compiler.core.adapter;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.analyzer.ExternalPortAnalyzer;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.AnalyzerContext;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * An adapter for {@link ExternalPortAnalyzer}.
 */
public class ExternalPortAnalyzerAdapter implements ExternalPortAnalyzer {

    private final ExternalPortProcessor processor;

    private final ExternalPortProcessor.AnalyzeContext context;

    /**
     * Creates a new instance.
     * @param context the delegate context
     */
    public ExternalPortAnalyzerAdapter(AnalyzerContext context) {
        this.processor = context.getTools().getExternalPortProcessor();
        this.context = new ContextAdapter(context);
    }

    /**
     * Creates a new instance.
     * @param processor the delegate processor
     * @param context the delegate context
     */
    public ExternalPortAnalyzerAdapter(ExternalPortProcessor processor, ExternalPortProcessor.AnalyzeContext context) {
        this.processor = processor;
        this.context = context;
    }

    @Override
    public ExternalInputInfo analyze(String name, ImporterDescription description) {
        if (processor.isSupported(context, description.getClass())) {
            return processor.analyzeInput(context, name, description);
        }
        throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                "unsupported external input: {0} ({1})",
                name,
                description.getClass().getName()));
    }

    @Override
    public ExternalOutputInfo analyze(String name, ExporterDescription description) {
        if (processor.isSupported(context, description.getClass())) {
            return processor.analyzeOutput(context, name, description);
        }
        throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                "unsupported external output: {0} ({1})",
                name,
                description.getClass().getName()));
    }

    private static final class ContextAdapter implements ExternalPortProcessor.AnalyzeContext {

        private final AnalyzerContext delegate;

        private final DataModelLoader dataModelLoader;

        public ContextAdapter(AnalyzerContext delegate) {
            this.delegate = delegate;
            this.dataModelLoader = new DataModelLoaderAdapter(delegate);
        }

        @Override
        public ClassLoader getClassLoader() {
            return delegate.getProject().getClassLoader();
        }

        @Override
        public DataModelLoader getDataModelLoader() {
            return dataModelLoader;
        }
    }
}
