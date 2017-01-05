/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents DAG of operators.
 * @since 0.1.0
 * @version 0.3.1
 */
public class OperatorGraph {

    private final Set<Operator> operators = new HashSet<>();

    /**
     * Creates a new instance.
     */
    public OperatorGraph() {
        return;
    }

    /**
     * Creates a new instance.
     * @param operators the member operators
     */
    public OperatorGraph(Collection<? extends Operator> operators) {
        this.operators.addAll(operators);
    }

    /**
     * Adds an operator to this graph.
     * @param operator the target operator
     * @return this
     * @see #rebuild()
     */
    public OperatorGraph add(Operator operator) {
        operators.add(operator);
        return this;
    }

    /**
     * Removes the operator from this graph.
     * If the operator is reachable from other operators in this graph,
     * the {@link #rebuild()} may restore the removed operator.
     * @see Operator#disconnectAll()
     * @param operator the target operator
     * @return this
     */
    public OperatorGraph remove(Operator operator) {
        operators.remove(operator);
        return this;
    }

    /**
     * Returns whether this graph explicitly contains the target operator.
     * If the operator is reachable from other operators in this graph,
     * the {@link #rebuild()} may restore the removed operator.
     * @param operator the target operator
     * @return {@code true} if this contains the target operator, otherwise {@code false}
     */
    public boolean contains(Operator operator) {
        return operators.contains(operator);
    }

    /**
     * Removes all operators in this graph.
     * @return this
     */
    public OperatorGraph clear() {
        operators.clear();
        return this;
    }

    /**
     * Returns operators in this graph.
     * If connections between operators are changed, clients may
     * {@link #rebuild() rebuild this graph} as well before invokes this method.
     * @return the thin copy of operators in this graph
     * @see #rebuild()
     * @see #getAllOperators(Collection)
     */
    public Collection<Operator> getOperators() {
        return getOperators(true);
    }

    /**
     * Returns operators in this graph.
     * If connections between operators are changed, clients may
     * {@link #rebuild() rebuild this graph} as well before invokes this method.
     * @param copy {@code true} to create a thin copy of operators
     * @return the operators in this graph
     * @see #rebuild()
     * @see #getAllOperators(Collection)
     */
    public Collection<Operator> getOperators(boolean copy) {
        if (copy) {
            return new ArrayList<>(operators);
        } else {
            return Collections.unmodifiableSet(operators);
        }
    }

    /**
     * Returns {@link ExternalInput} map (which key is its name) in this graph.
     * If connections between operators are changed, clients may
     * {@link #rebuild() rebuild this graph} as well before invokes this method.
     * @return this
     * @see #rebuild()
     */
    public Map<String, ExternalInput> getInputs() {
        return getPorts(ExternalInput.class);
    }

    /**
     * Returns {@link ExternalOutput} map (which key is its name) in this graph.
     * If connections between operators are changed, clients may
     * {@link #rebuild() rebuild this graph} as well before invokes this method.
     * @return this
     * @see #rebuild()
     */
    public Map<String, ExternalOutput> getOutputs() {
        return getPorts(ExternalOutput.class);
    }

    private <T extends ExternalPort> Map<String, T> getPorts(Class<T> type) {
        Map<String, T> results = new HashMap<>();
        for (Operator operator : operators) {
            if (type.isInstance(operator)) {
                T port = type.cast(operator);
                results.put(port.getName(), port);
            }
        }
        return results;
    }

    /**
     * Collects reachable operators from members in this graph, and adds them to this.
     * @return this
     * @see #getAllOperators(Collection)
     */
    public OperatorGraph rebuild() {
        Set<Operator> newOperators = getAllOperators(operators);
        this.operators.clear();
        this.operators.addAll(newOperators);
        return this;
    }

    /**
     * Returns the copy of this graph.
     * Each operator in this graph is {@link Operator#copy() copied} recursively (includes all reachable operators),
     * and connected each other.
     * @return the copy of this graph
     * @see #copy(Collection)
     */
    public OperatorGraph copy() {
        Map<Operator, Operator> map = copy(getAllOperators(operators));
        Collection<Operator> results = new ArrayList<>();
        for (Operator operator : operators) {
            Operator mapped = map.get(operator);
            assert mapped != null;
            results.add(mapped);
        }
        return new OperatorGraph(results);
    }

    /**
     * Returns a new current snapshot for this object.
     * @return the created snapshot
     * @since 0.3.1
     */
    public Snapshot getSnapshot() {
        return Snapshot.of(operators);
    }

    /**
     * Returns all operators which are reachable from the specified operators.
     * @param operators the operators
     * @return the all operators
     */
    public static Set<Operator> getAllOperators(Collection<? extends Operator> operators) {
        return Operators.getTransitiveConnected(operators);
    }

    /**
     * Returns a copy of operators.
     * Each operator is {@link Operator#copy() copied} recursively,
     * and it connected each other only if the neighbor operators are also copied in this invocation.
     * @param operators the target operators (maps an original operator into its copy.
     * @return a map of original operator to the corresponded copy
     */
    public static Map<Operator, Operator> copy(Collection<? extends Operator> operators) {
        Map<Operator, Operator> map = new HashMap<>();
        Map<OperatorInput, OperatorInput> inputs = new HashMap<>();
        Map<OperatorOutput, OperatorOutput> outputs = new HashMap<>();
        for (Operator operator : operators) {
            Operator copy = operator.copy();
            assert operator.getOriginalSerialNumber() == copy.getOriginalSerialNumber();
            map.put(operator, copy);
            List<? extends OperatorProperty> from = operator.getProperties();
            List<? extends OperatorProperty> to = copy.getProperties();
            assert from.size() == to.size();
            for (int i = 0, n = from.size(); i < n; i++) {
                OperatorProperty source = from.get(i);
                OperatorProperty target = to.get(i);
                assert source.getPropertyKind() == target.getPropertyKind();
                switch (source.getPropertyKind()) {
                case INPUT:
                    inputs.put((OperatorInput) source, (OperatorInput) target);
                    break;
                case OUTPUT:
                    outputs.put((OperatorOutput) source, (OperatorOutput) target);
                    break;
                default:
                    break;
                }
            }
        }
        for (Map.Entry<OperatorInput, OperatorInput> entry : inputs.entrySet()) {
            OperatorInput from = entry.getKey();
            OperatorInput to = entry.getValue();
            assert to.hasOpposites() == false;
            for (OperatorOutput upstream : from.getOpposites()) {
                if (outputs.containsKey(upstream)) {
                    OperatorOutput mapped = outputs.get(upstream);
                    to.connect(mapped);
                }
            }
        }
        return map;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "OperatorGraph({0}operators)", //$NON-NLS-1$
                operators.size());
    }

    /**
     * Represents a snapshot of {@link OperatorGraph}.
     * @since 0.3.1
     */
    public static final class Snapshot {

        private final Set<Operator> vertices;

        private final Set<Edge> edges;

        private Snapshot(Set<Operator> vertices, Set<Edge> edges) {
            this.vertices = vertices;
            this.edges = edges;
        }

        static Snapshot of(Collection<? extends Operator> operators) {
            Set<Operator> vertices = new HashSet<>();
            Set<Edge> edges = new HashSet<>();
            for (Operator operator : operators) {
                vertices.add(operator);
                for (OperatorOutput upstream : operator.getOutputs()) {
                    for (OperatorInput downstream : upstream.getOpposites()) {
                        edges.add(new Edge(upstream, downstream));
                    }
                }
            }
            return new Snapshot(vertices, edges);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hashCode(vertices);
            result = prime * result + Objects.hashCode(edges);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Snapshot other = (Snapshot) obj;
            if (!Objects.equals(vertices, other.vertices)) {
                return false;
            }
            if (!Objects.equals(edges, other.edges)) {
                return false;
            }
            return true;
        }
    }

    private static class Edge {

        final OperatorOutput upstream;

        final OperatorInput downstream;

        Edge(OperatorOutput upstream, OperatorInput downstream) {
            assert upstream != null;
            assert downstream != null;
            this.upstream = upstream;
            this.downstream = downstream;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hashCode(upstream);
            result = prime * result + Objects.hashCode(downstream);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Edge other = (Edge) obj;
            if (!Objects.equals(upstream, other.upstream)) {
                return false;
            }
            if (!Objects.equals(downstream, other.downstream)) {
                return false;
            }
            return true;
        }
    }
}
