/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.api.model;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.asakusafw.lang.utils.common.Arguments;

/**
 * Describes behavior or vertices.
 * @since 0.4.0
 */
public class VertexInfo implements Serializable {

    private static final long serialVersionUID = 2L;

    private final VertexId id;

    private final VertexDescriptor descriptor;

    private final List<PortInfo> inputs = new ArrayList<>();

    private final List<PortInfo> outputs = new ArrayList<>();

    /**
     * Creates a new instance.
     * @param id the vertex ID
     * @param descriptor the descriptor
     */
    public VertexInfo(VertexId id, VertexDescriptor descriptor) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(descriptor);
        this.id = id;
        this.descriptor = descriptor;
    }

    /**
     * Returns the vertex ID.
     * @return the vertex ID
     */
    public VertexId getId() {
        return id;
    }

    /**
     * Returns the descriptor how processes this vertex.
     * @return the descriptor
     */
    public VertexDescriptor getDescriptor() {
        return descriptor;
    }

    /**
     * Returns the vertex name.
     * @return the vertex name
     */
    public String getName() {
        return getId().getName();
    }

    /**
     * Adds a new input port to this vertex.
     * @param name the port name
     * @return the added port
     */
    public PortInfo addInputPort(String name) {
        return addInputPort(name, null);
    }

    /**
     * Adds a new input port to this vertex.
     * @param name the port name
     * @param tag the optional port tag (nullable)
     * @return the added port
     */
    public PortInfo addInputPort(String name, String tag) {
        Arguments.requireNonNull(name);
        return addPort(inputs, name, PortInfo.Direction.INPUT, tag);
    }

    /**
     * Adds a new output port to this vertex.
     * @param name the port name
     * @return the added port
     */
    public PortInfo addOutputPort(String name) {
        return addOutputPort(name, null);
    }

    /**
     * Adds a new output port to this vertex.
     * @param name the port name
     * @param tag the optional port tag (nullable)
     * @return the added port
     */
    public PortInfo addOutputPort(String name, String tag) {
        Arguments.requireNonNull(name);
        return addPort(outputs, name, PortInfo.Direction.OUTPUT, tag);
    }

    private PortInfo addPort(List<PortInfo> target, String name, PortInfo.Direction direction, String tag) {
        if (target.stream().anyMatch(o -> o.getName().equals(name))) {
            throw new IllegalStateException(MessageFormat.format(
                    "duplicate port: {1}:{2} ({0})", //$NON-NLS-1$
                    id,
                    name,
                    direction));
        }
        PortInfo info = new PortInfo(new PortId(id, name, direction), tag);
        target.add(info);
        return info;
    }

    /**
     * Returns the input ports which can obtain upstream data-sets.
     * @return the input ports
     */
    public List<PortInfo> getInputPorts() {
        return Collections.unmodifiableList(inputs);
    }

    /**
     * Returns the output ports which can submit downstream data-sets.
     * @return the output ports
     */
    public List<PortInfo> getOutputPorts() {
        return Collections.unmodifiableList(outputs);
    }

    /**
     * Returns an input port with the specified name.
     * @param name the target name
     * @return the found port
     */
    public Optional<PortInfo> findInputPort(String name) {
        return findPort(inputs, name);
    }

    /**
     * Returns an input port with the specified name.
     * @param name the target name
     * @return the found port
     */
    public Optional<PortInfo> findOutputPort(String name) {
        return findPort(outputs, name);
    }

    private static Optional<PortInfo> findPort(List<PortInfo> target, String name) {
        return target.stream().filter(o -> o.getName().equals(name)).findFirst();
    }
}
