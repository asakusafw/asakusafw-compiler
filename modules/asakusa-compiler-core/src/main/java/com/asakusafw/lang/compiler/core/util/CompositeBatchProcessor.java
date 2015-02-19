package com.asakusafw.lang.compiler.core.util;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;

/**
 * Composition of {@link BatchProcessor}s.
 */
public class CompositeBatchProcessor implements BatchProcessor, CompositeElement<BatchProcessor> {

    private final List<BatchProcessor> elements;

    /**
     * Creates a new instance.
     * @param elements the element processors
     */
    public CompositeBatchProcessor(List<? extends BatchProcessor> elements) {
        this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
    }

    @Override
    public List<BatchProcessor> getElements() {
        return elements;
    }

    /**
     * Composites the element processors.
     * @param elements the element processors
     * @return the composite processor
     */
    public static BatchProcessor composite(List<? extends BatchProcessor> elements) {
        if (elements.size() == 1) {
            return elements.get(0);
        }
        return new CompositeBatchProcessor(elements);
    }

    @Override
    public void process(Context context, BatchReference source) throws IOException {
        boolean error = false;
        List<Diagnostic> diagnostics = new ArrayList<>();
        for (BatchProcessor element : elements) {
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
