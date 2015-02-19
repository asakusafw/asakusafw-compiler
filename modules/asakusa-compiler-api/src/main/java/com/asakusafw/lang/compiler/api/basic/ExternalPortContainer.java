package com.asakusafw.lang.compiler.api.basic;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;

/**
 * Holds {@link ExternalInputReference}s and {@link ExternalOutputReference}s.
 */
public class ExternalPortContainer {

    private final Map<String, ExternalInputReference> inputs = new LinkedHashMap<>();

    private final Map<String, ExternalOutputReference> outputs = new LinkedHashMap<>();

    /**
     * Adds an external input to this container.
     * @param reference the external input reference
     */
    public void addInput(ExternalInputReference reference) {
        String name = reference.getName();
        if (inputs.containsKey(name)) {
            throw new IllegalStateException(MessageFormat.format(
                    "external input is already declared in this jobflow: \"{0}\" ({1})",
                    name,
                    reference.getDescriptionClass().getName()));
        }
        inputs.put(name, reference);
    }

    /**
     * Adds an external output to this container.
     * @param reference the external output reference
     */
    public void addOutput(ExternalOutputReference reference) {
        String name = reference.getName();
        if (outputs.containsKey(name)) {
            throw new IllegalStateException(MessageFormat.format(
                    "external output is already declared in this jobflow: \"{0}\" ({1})",
                    name,
                    reference.getDescriptionClass().getName()));
        }
        outputs.put(name, reference);
    }

    /**
     * Returns the external inputs which {@link #addInput(ExternalInputReference) added} to this container.
     * @return the added external inputs
     */
    public List<ExternalInputReference> getInputs() {
        return new ArrayList<>(inputs.values());
    }

    /**
     * Returns the external outputs which {@link #addOutput(ExternalOutputReference) added} to this container.
     * @return the added external outputs
     */
    public List<ExternalOutputReference> getOutputs() {
        return new ArrayList<>(outputs.values());
    }

    /**
     * Returns whether this container is empty or not.
     * @return {@code true} if this container has no external ports, otherwise {@code false}
     */
    public boolean isEmpty() {
        return inputs.isEmpty() && outputs.isEmpty();
    }
}
