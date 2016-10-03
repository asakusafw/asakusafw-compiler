/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.model.build;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.asakusafw.dag.api.model.EdgeDescriptor;
import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.api.model.PortInfo;
import com.asakusafw.dag.api.model.VertexInfo;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * Builds {@link GraphInfo}.
 * @since 0.4.0
 */
public class GraphInfoBuilder {

    /**
     * The implicit I/O port name.
     */
    public static final String NAME_IMPLICIT_PORT = "_dependency"; //$NON-NLS-1$

    private final Map<String, ResolvedVertexInfo> vertices = new LinkedHashMap<>();

    private final Map<SubPlan, ResolvedVertexInfo> vertexMap = new LinkedHashMap<>();

    private final Map<SubPlan.Input, ResolvedInputInfo> inputMap = new LinkedHashMap<>();

    private final Map<SubPlan.Output, ResolvedOutputInfo> outputMap = new LinkedHashMap<>();

    /**
     * Adds a vertex.
     * @param vertex the target vertex
     */
    public void add(ResolvedVertexInfo vertex) {
        Arguments.requireNonNull(vertex);
        String id = vertex.getId();
        Invariants.require(vertices.containsKey(id) == false, id);
        Invariants.require(vertex.getInputs().keySet().stream().noneMatch(inputMap::containsKey));
        Invariants.require(vertex.getOutputs().keySet().stream().noneMatch(outputMap::containsKey));
        vertices.put(id, vertex);
        inputMap.putAll(vertex.getInputs());
        outputMap.putAll(vertex.getOutputs());
    }

    /**
     * Adds a vertex which has the related sub-plan.
     * @param member the related sub-plan
     * @param vertex the target vertex
     */
    public void add(SubPlan member, ResolvedVertexInfo vertex) {
        Arguments.requireNonNull(member);
        Arguments.requireNonNull(vertex);
        Invariants.require(vertexMap.containsKey(member) == false, () -> member);
        add(vertex);
        vertexMap.put(member, vertex);
    }

    /**
     * Returns a vertex which has the target ID.
     * @param id the vertex ID
     * @return the related vertex, or {@code null} if it is not defined
     */
    public ResolvedVertexInfo get(String id) {
        Arguments.requireNonNull(id);
        return vertices.get(id);
    }

    /**
     * Returns a vertex which is related to the target sub-plan.
     * @param member the target sub-plan
     * @return the related vertex, or {@code null} if it is not defined
     */
    public ResolvedVertexInfo get(SubPlan member) {
        Arguments.requireNonNull(member);
        return vertexMap.get(member);
    }

    /**
     * Returns an input which is related to the target port.
     * @param port the target port
     * @return the related input
     */
    public ResolvedInputInfo get(SubPlan.Input port) {
        return inputMap.get(port);
    }

    /**
     * Returns an output which is related to the target port.
     * @param port the target port
     * @return the related output
     */
    public ResolvedOutputInfo get(SubPlan.Output port) {
        return outputMap.get(port);
    }

    /**
     * Builds a {@link GraphInfo}.
     * @param voidEdge a supplier which provides an edge descriptor for implicit dependencies
     * @return the built object
     */
    public GraphInfo build(Supplier<? extends EdgeDescriptor> voidEdge) {
        GraphInfo info = new GraphInfo();
        Map<ResolvedVertexInfo, VertexInfo> vs = new LinkedHashMap<>();
        Map<ResolvedInputInfo, PortInfo> is = new LinkedHashMap<>();
        Map<ResolvedOutputInfo, PortInfo> os = new LinkedHashMap<>();
        for (ResolvedVertexInfo s : vertices.values()) {
            VertexInfo v = info.addVertex(s.getId(), s.getDescriptor());
            vs.put(s, v);
            for (ResolvedOutputInfo p : s.getOutputs().values()) {
                if (os.containsKey(p) == false) {
                    os.put(p, v.addOutputPort(p.getId(), p.getTag()));
                }
            }
            for (ResolvedInputInfo p : s.getInputs().values()) {
                if (is.containsKey(p) == false) {
                    is.put(p, v.addInputPort(p.getId(), p.getTag()));
                }
            }
        }
        connectImplicitDependencies(info, vs, voidEdge);
        connectEdges(info, is, os);
        return info;
    }

    private static void connectEdges(GraphInfo info,
            Map<ResolvedInputInfo, PortInfo> is, Map<ResolvedOutputInfo, PortInfo> os) {
        for (Map.Entry<ResolvedOutputInfo, PortInfo> entry : os.entrySet()) {
            Set<ResolvedInputInfo> targets = entry.getKey().getDownstreams();
            if (targets.isEmpty()) {
                continue;
            }
            // FIXME check - each descriptor must be compatible
            EdgeDescriptor descriptor = targets.stream()
                .map(ResolvedInputInfo::getDescriptor)
                .findFirst()
                .orElseThrow(IllegalStateException::new);
            PortInfo upstream = entry.getValue();
            targets.stream()
                .map(is::get)
                .forEach(downstream -> info.addEdge(upstream.getId(), downstream.getId(), descriptor));
        }
    }

    private static void connectImplicitDependencies(GraphInfo info,
            Map<ResolvedVertexInfo, VertexInfo> vs,
            Supplier<? extends EdgeDescriptor> voidEdge) {
        Set<ResolvedVertexInfo> targets = vs.keySet().stream()
                .flatMap(v -> v.getImplicitDependencies().stream())
                .collect(Collectors.toSet());
        targets.stream()
            .map(vs::get)
            .forEach(v -> v.addOutputPort(NAME_IMPLICIT_PORT));
        for (Map.Entry<ResolvedVertexInfo, VertexInfo> entry : vs.entrySet()) {
            ResolvedVertexInfo s = entry.getKey();
            Set<PortInfo> upstreams = s.getImplicitDependencies().stream()
                    .map(vs::get)
                    .map(v -> Invariants.requirePresent(v.findOutputPort(NAME_IMPLICIT_PORT)))
                    .collect(Collectors.toSet());
            if (upstreams.isEmpty()) {
                continue;
            }
            VertexInfo v = entry.getValue();
            PortInfo downstream = v.addInputPort(NAME_IMPLICIT_PORT);
            for (PortInfo upstream : upstreams) {
                info.addEdge(upstream.getId(), downstream.getId(), voidEdge.get());
            }
        }
    }
}
