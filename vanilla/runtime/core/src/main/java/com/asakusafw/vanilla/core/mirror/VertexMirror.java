/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.vanilla.core.mirror;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.dag.api.model.PortInfo;
import com.asakusafw.dag.api.model.VertexId;
import com.asakusafw.dag.api.model.VertexInfo;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor;
import com.asakusafw.dag.api.model.basic.BasicVertexDescriptor;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * Represents a vertex.
 * @since 0.4.0
 */
public final class VertexMirror {

    private final VertexId id;

    private final BasicVertexDescriptor descriptor;

    private final Map<String, InputPortMirror> inputs = new LinkedHashMap<>();

    private final Map<String, OutputPortMirror> outputs = new LinkedHashMap<>();

    /**
     * Creates a new instance without empty I/O ports.
     * @param info the original information
     */
    public VertexMirror(VertexInfo info) {
        Arguments.requireNonNull(info);
        this.id = info.getId();
        this.descriptor = (BasicVertexDescriptor) info.getDescriptor();
    }

    /**
     * Returns the ID of this.
     * @return the ID
     */
    public VertexId getId() {
        return id;
    }

    /**
     * Returns the input port.
     * @param portName the port name
     * @return the input port
     */
    public InputPortMirror getInput(String portName) {
        return Invariants.requireNonNull(inputs.get(portName));
    }

    /**
     * Returns the output port.
     * @param portName the port name
     * @return the output port
     */
    public OutputPortMirror getOutput(String portName) {
        return Invariants.requireNonNull(outputs.get(portName));
    }

    /**
     * Returns the input ports.
     * @return the input ports
     */
    public Collection<InputPortMirror> getInputs() {
        return Collections.unmodifiableCollection(inputs.values());
    }

    /**
     * Returns the output ports.
     * @return the output ports
     */
    public Collection<OutputPortMirror> getOutputs() {
        return Collections.unmodifiableCollection(outputs.values());
    }

    /**
     * Creates a new vertex processor.
     * @param loader the class loader
     * @return the created processor
     */
    public VertexProcessor newProcessor(ClassLoader loader) {
        Arguments.requireNonNull(loader);
        return (VertexProcessor) descriptor.getProcessor().newInstance(loader).get();
    }

    /**
     * Adds an input port.
     * @param port the target port information
     * @param edge the corresponded edge information
     * @return the created port
     */
    public InputPortMirror addInput(PortInfo port, BasicEdgeDescriptor edge) {
        Arguments.requireNonNull(port);
        Arguments.requireNonNull(edge);
        return addPort(new InputPortMirror(this, port, edge), inputs);
    }

    /**
     * Adds an output port.
     * @param port the target port information
     * @param edge the corresponded edge information
     * @return the created port
     */
    public OutputPortMirror addOutput(PortInfo port, BasicEdgeDescriptor edge) {
        Arguments.requireNonNull(port);
        Arguments.requireNonNull(edge);
        return addPort(new OutputPortMirror(this, port, edge), outputs);
    }

    private static <T extends PortMirror> T addPort(T port, Map<String, T> destination) {
        String portName = port.getId().getName();
        Invariants.require(destination.containsKey(portName) == false, port::getId);
        destination.put(portName, port);
        return port;
    }

    @Override
    public String toString() {
        return getId().toString();
    }
}
