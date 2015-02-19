package com.asakusafw.lang.compiler.core.util;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;

/**
 * Composition of {@link ExternalPortProcessor}s.
 */
public class CompositeExternalPortProcessor implements ExternalPortProcessor, CompositeElement<ExternalPortProcessor> {

    private final List<ExternalPortProcessor> elements;

    /**
     * Creates a new instance.
     * @param elements the element processors
     */
    public CompositeExternalPortProcessor(List<? extends ExternalPortProcessor> elements) {
        this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
    }

    @Override
    public List<ExternalPortProcessor> getElements() {
        return elements;
    }

    /**
     * Composites the element processors.
     * @param elements the element processors
     * @return the composite processor
     */
    public static ExternalPortProcessor composite(List<? extends ExternalPortProcessor> elements) {
        if (elements.size() == 1) {
            return elements.get(0);
        }
        return new CompositeExternalPortProcessor(elements);
    }

    private ExternalPortProcessor getSupported(AnalyzeContext context, Class<?> descriptionClass) {
        for (ExternalPortProcessor element : elements) {
            if (element.isSupported(context, descriptionClass)) {
                return element;
            }
        }
        return null;
    }

    private ExternalPortProcessor getSupportedChecked(AnalyzeContext context, Class<?> descriptionClass) {
        ExternalPortProcessor supported = getSupported(context, descriptionClass);
        if (supported == null) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "unsupported external input/output description: {0}",
                    descriptionClass.getName()));
        }
        return supported;
    }

    private ExternalPortProcessor getSupportedChecked(Context context, ExternalPortInfo info) {
        try {
            Class<?> aClass = info.getDescriptionClass().resolve(context.getClassLoader());
            return getSupportedChecked(context, aClass);
        } catch (ReflectiveOperationException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to resolve a class: {0}",
                    info.getDescriptionClass().getName()), e);
        }
    }

    @Override
    public boolean isSupported(AnalyzeContext context, Class<?> descriptionClass) {
        return getSupported(context, descriptionClass) != null;
    }

    @Override
    public ExternalInputInfo analyzeInput(AnalyzeContext context, String name, Object description) {
        ExternalPortProcessor element = getSupportedChecked(context, description.getClass());
        return element.analyzeInput(context, name, description);
    }

    @Override
    public ExternalOutputInfo analyzeOutput(AnalyzeContext context, String name, Object description) {
        ExternalPortProcessor element = getSupportedChecked(context, description.getClass());
        return element.analyzeOutput(context, name, description);
    }

    @Override
    public ExternalInputReference resolveInput(Context context, String name, ExternalInputInfo info) {
        ExternalPortProcessor element = getSupportedChecked(context, info);
        return element.resolveInput(context, name, info);
    }

    @Override
    public ExternalOutputReference resolveOutput(Context context, String name, ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        ExternalPortProcessor element = getSupportedChecked(context, info);
        return element.resolveOutput(context, name, info, internalOutputPaths);
    }

    @Override
    public void process(Context context, List<ExternalInputReference> inputs, List<ExternalOutputReference> outputs)
            throws IOException {
        Map<ExternalPortProcessor, Pair> pairs = new LinkedHashMap<>();
        for (ExternalPortProcessor element : elements) {
            pairs.put(element, new Pair());
        }
        for (ExternalInputReference port : inputs) {
            ExternalPortProcessor supported = getSupportedChecked(context, port);
            assert pairs.containsKey(supported);
            pairs.get(supported).inputs.add(port);
        }
        for (ExternalOutputReference port : outputs) {
            ExternalPortProcessor supported = getSupportedChecked(context, port);
            assert pairs.containsKey(supported);
            pairs.get(supported).outputs.add(port);
        }
        for (Map.Entry<ExternalPortProcessor, Pair> entry : pairs.entrySet()) {
            ExternalPortProcessor element = entry.getKey();
            Pair pair = entry.getValue();
            if (pair.isValid()) {
                element.process(context, pair.inputs, pair.outputs);
            }
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

    private static final class Pair {

        final List<ExternalInputReference> inputs = new ArrayList<>();

        final List<ExternalOutputReference> outputs = new ArrayList<>();

        Pair() {
            return;
        }

        boolean isValid() {
            return inputs.isEmpty() == false || outputs.isEmpty() == false;
        }
    }
}
