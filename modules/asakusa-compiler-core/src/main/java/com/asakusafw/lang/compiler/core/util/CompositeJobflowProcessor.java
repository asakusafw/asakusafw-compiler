package com.asakusafw.lang.compiler.core.util;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Jobflow;

/**
 * Composition of {@link JobflowProcessor}s.
 */
public class CompositeJobflowProcessor implements JobflowProcessor, CompositeElement<JobflowProcessor> {

    private final List<JobflowProcessor> elements;

    /**
     * Creates a new instance.
     * @param elements the element processors
     */
    public CompositeJobflowProcessor(List<? extends JobflowProcessor> elements) {
        this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
    }

    @Override
    public List<JobflowProcessor> getElements() {
        return elements;
    }

    /**
     * Composites the element processors.
     * @param elements the element processors
     * @return the composite processor
     */
    public static JobflowProcessor composite(List<? extends JobflowProcessor> elements) {
        if (elements.size() == 1) {
            return elements.get(0);
        }
        return new CompositeJobflowProcessor(elements);
    }

    @Override
    public void process(Context context, Jobflow source) throws IOException {
        boolean error = false;
        List<Diagnostic> diagnostics = new ArrayList<>();
        for (JobflowProcessor element : elements) {
            try {
                element.process(context, source);
            } catch (DiagnosticException e) {
                error = true;
                diagnostics.addAll(e.getDiagnostics());
            }
        }
        if (error) {
            throw new DiagnosticException(diagnostics);
        }
    }

    @Override
    public String toString() {
        if (elements.isEmpty()) {
            return "NULL"; //$NON-NLS-1$
        }
        return MessageFormat.format(
                "Composite{0}", //$NON-NLS-1$
                elements);
    }
}
