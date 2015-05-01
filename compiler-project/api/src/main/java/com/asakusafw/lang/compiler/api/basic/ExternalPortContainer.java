/**
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.lang.compiler.api.basic;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalPortReferenceMap;

/**
 * Holds {@link ExternalInputReference}s and {@link ExternalOutputReference}s.
 */
public class ExternalPortContainer implements ExternalPortReferenceMap {

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
                    reference.getDescriptionClass().getClassName()));
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
                    reference.getDescriptionClass().getClassName()));
        }
        outputs.put(name, reference);
    }

    /**
     * Returns an external input.
     * @param name the target input port name
     * @return the external input, or {@code null} if there is no such an external input
     */
    @Override
    public ExternalInputReference findInput(String name) {
        return inputs.get(name);
    }

    /**
     * Returns an external output.
     * @param name the target output port name
     * @return the external output, or {@code null} if there is no such an external output
     */
    @Override
    public ExternalOutputReference findOutput(String name) {
        return outputs.get(name);
    }

    /**
     * Returns the external inputs which {@link #addInput(ExternalInputReference) added} to this container.
     * @return the added external inputs
     */
    @Override
    public List<ExternalInputReference> getInputs() {
        return new ArrayList<>(inputs.values());
    }

    /**
     * Returns the external outputs which {@link #addOutput(ExternalOutputReference) added} to this container.
     * @return the added external outputs
     */
    @Override
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
