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
package com.asakusafw.vanilla.core.mirror;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.api.model.PortId;
import com.asakusafw.dag.api.model.PortInfo.Direction;
import com.asakusafw.dag.api.model.VertexId;
import com.asakusafw.dag.api.model.VertexInfo;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * Represents a graph.
 * @since 0.4.0
 */
public class GraphMirror {

    private final Map<String, VertexMirror> vertices = new LinkedHashMap<>();

    /**
     * Creates a new instance from the original information.
     * @param info the original information
     * @return the created instance
     */
    public static GraphMirror of(GraphInfo info) {
        GraphMirror result = new GraphMirror();
        Map<PortId, BasicEdgeDescriptor> ports = new HashMap<>();
        info.getEdges().stream()
            .flatMap(e -> Stream.of(new Tuple<>(e.getUpstreamId(), e), new Tuple<>(e.getDownstreamId(), e)))
            .map(t -> new Tuple<>(t.left(), (BasicEdgeDescriptor) t.right().getDescriptor()))
            .forEach(t -> ports.merge(t.left(), t.right(), (a, b) -> {
                Invariants.require(a.equals(b), () -> MessageFormat.format(
                        "conflict edge type on {0}: {1} <=> {2}",
                        t.left(), a, b));
                return a;
            }));
        info.getVertices().forEach(result::addVertex);
        info.getVertices().forEach(v -> {
            VertexMirror vertex = result.getVertex(v.getId());
            v.getInputPorts().forEach(p -> vertex.addInput(p, ports.get(p.getId())));
            v.getOutputPorts().forEach(p -> vertex.addOutput(p, ports.get(p.getId())));
        });
        info.getEdges().forEach(e -> {
            result.getOutput(e.getUpstreamId()).connect(result.getInput(e.getDownstreamId()));
        });
        return result;
    }

    /**
     * Returns the vertex.
     * @param vertexName the vertex name
     * @return the vertex
     */
    public VertexMirror getVertex(String vertexName) {
        Arguments.requireNonNull(vertexName);
        return Invariants.requireNonNull(vertices.get(vertexName));
    }

    /**
     * Returns the vertex.
     * @param id the vertex ID
     * @return the vertex
     */
    public VertexMirror getVertex(VertexId id) {
        Arguments.requireNonNull(id);
        return getVertex(id.getName());
    }

    /**
     * Returns the vertices.
     * @return the vertices
     */
    public Collection<VertexMirror> getVertices() {
        return Collections.unmodifiableCollection(vertices.values());
    }

    /**
     * Returns the input port.
     * @param id the port ID
     * @return the input port
     */
    public InputPortMirror getInput(PortId id) {
        Arguments.requireNonNull(id);
        Arguments.require(id.getDirection() == Direction.INPUT);
        return getVertex(id.getVertexId()).getInput(id.getName());
    }

    /**
     * Returns the output port.
     * @param id the port ID
     * @return the output port
     */
    public OutputPortMirror getOutput(PortId id) {
        Arguments.requireNonNull(id);
        Arguments.require(id.getDirection() == Direction.OUTPUT);
        return getVertex(id.getVertexId()).getOutput(id.getName());
    }

    /**
     * Adds a vertex.
     * @param info the original information
     * @return the added vertex
     */
    public VertexMirror addVertex(VertexInfo info) {
        Arguments.requireNonNull(info);
        String name = info.getId().getName();
        Invariants.require(vertices.containsKey(name) == false);
        VertexMirror result = new VertexMirror(info);
        vertices.put(name, result);
        return result;
    }
}
