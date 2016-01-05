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
package com.asakusafw.lang.compiler.planning;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.common.Predicate;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.planning.basic.PrimitivePlanner;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Utilities for execution planning.
 * @see Operators
 * @see PlanMarkers
 */
public final class Planning {

    /**
     * A predicate only accepts plan markers.
     */
    public static final Predicate<Operator> PLAN_MARKERS = new Predicate<Operator>() {
        @Override
        public boolean apply(Operator argument) {
            return PlanMarkers.get(argument) != null;
        }
    };

    private Planning() {
        return;
    }

    /**
     * Normalizes the operator graph.
     * <p>
     * This includes following operations:
     * </p>
     * <ol>
     * <li> flatten operators (unnest {@link FlowOperator}s, and remove non-external inputs/outputs) </li>
     * <li> add a pseudo input port for {@link ExternalInput}s </li>
     * <li> add a pseudo output port for {@link ExternalOutput}s </li>
     * <li> add {@link OperatorConstraint#GENERATOR generator constraint} to {@link ExternalInput}s </li>
     * <li> add {@link OperatorConstraint#AT_LEAST_ONCE at-least-once constraint} to {@link ExternalOutput}s </li>
     * <li> add {@link PlanMarker#BEGIN BEGIN} markers to input ports without any opposites </li>
     * <li> add {@link PlanMarker#END END} markers to output ports without any opposites </li>
     * </ol>
     * <p>
     * This will directly modify the specified operator graph.
     * </p>
     * @param graph the target operator graph
     * @see OperatorGraph#copy()
     */
    public static void normalize(OperatorGraph graph) {
        flatten(graph);
        enhanceExternalPorts(graph);
        insertTerminatorsForPorts(graph);
    }

    private static void flatten(OperatorGraph graph) {
        graph.rebuild();
        for (Operator operator : graph.getOperators()) {
            if (operator.getOperatorKind() == OperatorKind.FLOW) {
                flatten0((FlowOperator) operator);
                graph.remove(operator);
            }
        }
    }

    private static void flatten0(FlowOperator flow) {
        OperatorGraph graph = flow.getOperatorGraph().rebuild();

        // redirect flow I/Os
        Map<String, ExternalInput> inputs = graph.getInputs();
        for (OperatorInput outer : flow.getInputs()) {
            ExternalInput inner = inputs.get(outer.getName());
            assert inner.isExternal() == false;
            Operators.connectAll(outer.getOpposites(), inner.getOperatorPort().getOpposites());
            inner.disconnectAll();
            outer.disconnectAll();
        }
        Map<String, ExternalOutput> outputs = graph.getOutputs();
        for (OperatorOutput outer : flow.getOutputs()) {
            ExternalOutput inner = outputs.get(outer.getName());
            assert inner.isExternal() == false;
            Operators.connectAll(inner.getOperatorPort().getOpposites(), outer.getOpposites());
            inner.disconnectAll();
            outer.disconnectAll();
        }
        flow.disconnectAll();

        // remove flow I/Os
        for (ExternalInput port : inputs.values()) {
            if (port.isExternal()) {
                continue;
            }
            port.disconnectAll();
            graph.remove(port);
        }
        for (ExternalOutput port : outputs.values()) {
            if (port.isExternal()) {
                continue;
            }
            port.disconnectAll();
            graph.remove(port);
        }

        // flatten recursively
        for (Operator operator : graph.getOperators()) {
            if (operator.getOperatorKind() == OperatorKind.FLOW) {
                flatten0((FlowOperator) operator);
                graph.remove(operator);
            }
        }
    }

    private static void enhanceExternalPorts(OperatorGraph graph) {
        graph.rebuild();
        for (ExternalInput port : graph.getInputs().values()) {
            if (port.getInputs().isEmpty() == false) {
                continue;
            }
            OperatorOutput orig = port.getOperatorPort();
            ExternalInput.Builder builder = ExternalInput.builder(port.getName(), port.getInfo())
                    .input(ExternalInput.PORT_NAME, port.getDataType()) // virtual
                    .output(orig.getName(), orig.getDataType())
                    .constraint(port.getConstraints())
                    .constraint(OperatorConstraint.GENERATOR);
            copyAttributes(port, builder);
            ExternalInput enhanced = builder.build();
            Operators.connectAll(enhanced.getOperatorPort(), orig.getOpposites());
            graph.remove(port.disconnectAll());
            graph.add(enhanced);
        }
        for (ExternalOutput port : graph.getOutputs().values()) {
            if (port.getOutputs().isEmpty() == false) {
                continue;
            }
            OperatorInput orig = port.getOperatorPort();
            ExternalOutput.Builder builder = ExternalOutput.builder(port.getName(), port.getInfo())
                    .input(orig.getName(), orig.getDataType(), orig.getGroup())
                    .output(ExternalOutput.PORT_NAME, orig.getDataType()) // virtual
                    .constraint(port.getConstraints())
                    .constraint(OperatorConstraint.AT_LEAST_ONCE);
            copyAttributes(port, builder);
            ExternalOutput enhanced = builder.build();
            Operators.connectAll(orig.getOpposites(), enhanced.getOperatorPort());
            graph.remove(port.disconnectAll());
            graph.add(enhanced);
        }
    }

    private static void copyAttributes(Operator operator, Operator.AbstractBuilder<?, ?> builder) {
        for (Class<?> type : operator.getAttributeTypes()) {
            putAttribtue(type, operator.getAttribute(type), builder);
        }
    }

    private static <T> void putAttribtue(Class<T> type, Object value, Operator.AbstractBuilder<?, ?> builder) {
        builder.attribute(type, type.cast(value));
    }

    private static void insertTerminatorsForPorts(OperatorGraph graph) {
        graph.rebuild();
        for (Operator operator : graph.getOperators()) {
            PlanMarker kind = PlanMarkers.get(operator);
            if (kind == PlanMarker.BEGIN || kind == PlanMarker.END) {
                continue;
            }
            for (OperatorInput port : operator.getInputs()) {
                if (port.hasOpposites() == false) {
                    MarkerOperator marker = PlanMarkers.insert(PlanMarker.BEGIN, port);
                    graph.add(marker);
                }
            }
            for (OperatorOutput port : operator.getOutputs()) {
                if (port.hasOpposites() == false) {
                    MarkerOperator marker = PlanMarkers.insert(PlanMarker.END, port);
                    graph.add(marker);
                }
            }
        }
    }

    /**
     * Removes dead-flows in the operator graph.
     * <p>
     * This requires following preconditions:
     * </p>
     * <ol>
     * <li> each {@code BEGIN} plan marker must NOT have any predecessors </li>
     * <li> each {@code END} plan marker must NOT have any successors </li>
     * <li> each input port of operator except {@code BEGIN} plan marker has at least one opposites </li>
     * <li> each output port of operator except {@code END} plan marker has at least one opposites </li>
     * </ol>
     * <p>
     * This ensures that the operator graph satisfies following properties:
     * </p>
     * <ol>
     * <li> all preconditions </li>
     * <li>
     *     each operator must be backward reachable to
     *     any operators with {@link OperatorConstraint#GENERATOR generator constraint}
     *     except {@code BEGIN} plan markers or an operator with generator constraint
     * </li>
     * <li>
     *     each operator must be forward reachable to
     *     any operators with {@link OperatorConstraint#AT_LEAST_ONCE at-least-once constraint}
     *     except {@code END} plan markers or an operator with at-least-once constraint
     * </li>
     * </ol>
     * <p>
     * This will directly modify the specified operator graph.
     * </p>
     *
     * @param graph the target operator graph
     * @see OperatorConstraint#AT_LEAST_ONCE
     * @see OperatorConstraint#GENERATOR
     * @see OperatorGraph#copy()
     */
    public static void removeDeadFlow(OperatorGraph graph) {
        graph.rebuild();
        Set<Operator> operators = new HashSet<>(graph.getOperators());

        // effective operators = (reachable to generator) \cap (reachable to at-least-once)
        Set<Operator> effective = new HashSet<>();
        effective.addAll(collectInputReachables(operators));
        effective.retainAll(collectOutputReachables(operators));
        if (effective.isEmpty()) {
            throw new IllegalStateException("there are no effective operators"); //$NON-NLS-1$
        }

        // add only effective operators (this may includes terminators)
        graph.clear();
        for (Operator operator : operators) {
            if (effective.contains(operator)) {
                graph.add(operator);
            } else {
                operator.disconnectAll();
            }
        }

        // restore dropped terminators
        insertTerminatorsForPorts(graph);
    }

    private static Set<Operator> collectInputReachables(Set<Operator> operators) {
        Set<Operator> results = new HashSet<>();
        Set<OperatorOutput> edges = new HashSet<>();
        for (Operator operator : operators) {
            if (operator.getConstraints().contains(OperatorConstraint.GENERATOR)) {
                results.add(operator);
                edges.addAll(operator.getOutputs());
            }
        }
        results.addAll(Operators.getTransitiveSuccessors(edges));
        return results;
    }

    private static Set<Operator> collectOutputReachables(Set<Operator> operators) {
        Set<Operator> results = new HashSet<>();
        Set<OperatorInput> edges = new HashSet<>();
        for (Operator operator : operators) {
            if (operator.getConstraints().contains(OperatorConstraint.AT_LEAST_ONCE)) {
                results.add(operator);
                edges.addAll(operator.getInputs());
            }
        }
        results.addAll(Operators.getTransitivePredecessors(edges));
        return results;
    }

    /**
     * Simplifies {@code BEGIN} and {@code END} plan markers.
     * <p>
     * This requires following preconditions:
     * </p>
     * <ol>
     * <li> each {@code BEGIN} plan marker must NOT have any predecessors </li>
     * <li> each {@code END} plan marker must NOT have any successors </li>
     * <li> each operator except {@code BEGIN} plan marker must have at least one predecessors </li>
     * <li> each operator except {@code END} plan marker must have at least one successors </li>
     * </ol>
     * <p>
     * This ensures that the operator graph satisfies following properties:
     * </p>
     * <ol>
     * <li> all preconditions </li>
     * <li> each operator has up to one {@code BEGIN} plan marker in the predecessors </li>
     * <li> each operator has up to one {@code END} plan marker in the successors </li>
     * <li> each operator which has {@code BEGIN} plan marker in its predecessor, it has no other predecessors </li>
     * <li> each operator which has {@code END} plan marker in its successors, it has no other successors </li>
     * </ol>
     * <p>
     * This will directly modify the specified operator graph.
     * </p>
     * @param graph the target operator graph
     * @see PlanMarker#BEGIN
     * @see PlanMarker#END
     * @see OperatorGraph#copy()
     */
    public static void simplifyTerminators(OperatorGraph graph) {
        graph.rebuild();
        for (Operator operator : graph.getOperators()) {
            simplifyTerminators(graph, PlanMarker.BEGIN, operator, operator.getInputs());
            simplifyTerminators(graph, PlanMarker.END, operator, operator.getOutputs());
        }
    }

    private static void simplifyTerminators(
            OperatorGraph graph,
            PlanMarker target,
            Operator operator,
            Collection<? extends OperatorPort> ports) {
        // operator is already removed by simplification
        if (graph.contains(operator) == false) {
            return;
        }
        // counting neighbors
        boolean sawOther = false;
        int terminators = 0;
        for (OperatorPort port : ports) {
            for (OperatorPort opposite : port.getOpposites()) {
                PlanMarker marker = PlanMarkers.get(opposite.getOwner());
                if (marker == target) {
                    terminators++;
                } else {
                    sawOther = true;
                }
            }
        }
        if (sawOther == false && terminators == 0 && PlanMarkers.get(operator) != target) {
            throw new IllegalStateException(MessageFormat.format(
                    "planning violation - operator does not have {0} terminators: {1}", //$NON-NLS-1$
                    target,
                    operator));
        }
        // is already simplest form
        if (terminators == 0 || terminators == 1 && sawOther == false) {
            return;
        }
        // keeps 1 terminator only if there are other neighbors
        boolean keep = sawOther == false;
        for (OperatorPort port : ports) {
            for (OperatorPort opposite : port.getOpposites()) {
                PlanMarker marker = PlanMarkers.get(opposite.getOwner());
                if (marker == target) {
                    if (keep == false) {
                        purge(graph, port, opposite);
                    }
                    keep = false;
                }
            }
        }
    }

    private static void purge(OperatorGraph graph, OperatorPort port, OperatorPort target) {
        if (port instanceof OperatorInput) {
            assert target instanceof OperatorOutput;
            ((OperatorInput) port).disconnect((OperatorOutput) target);
        } else {
            assert port instanceof OperatorOutput;
            assert target instanceof OperatorInput;
            ((OperatorOutput) port).disconnect((OperatorInput) target);
        }
        removeIfOrphan(target.getOwner(), graph);
    }

    private static void removeIfOrphan(Operator operator, OperatorGraph graph) {
        if (Operators.hasPredecessors(operator) == false && Operators.hasSuccessors(operator) == false) {
            graph.remove(operator);
        }
    }

    /**
     * Organizes a primitive plan for the operator graph from including plan markers.
     * <p>
     * Roughly, a primitive plan consists of a lot of <em>primitive</em> sub-plans,
     * that have minimal inputs, outputs, and operators.
     * So primitive plans are generally not efficient, clients should re-organize them.
     * </p>
     *
     * <p>
     * First, we introduce a several terms:
     * </p>
     * <!-- CHECKSTYLE:OFF JavadocStyle -->
     * <dl>
     * <dt> <em>gathering operator</em> </dt>
     * <dd>
     *   each operator, which predecessors contain a {@code GATHER} plan marker,
     *   is a <em>gathering operator</em>.
     * </dd>
     * <dt> <em>non-broadcasting operator</em> </dt>
     * <dd>
     *   each plan marker is just <em>non-broadcasting operator</em>.
     * </dd>
     * <dd>
     *   each operator, which nearest forward reachable plan markers contain other than {@code BROADCAST},
     *   is a <em>non-broadcasting operator</em>.
     * </dd>
     * <dt> <em>broadcast consumer</em> </dt>
     * <dd>
     *   each operator, which is nearest forward reachable <em>non-broadcasting operator</em>
     *   from {@code BROADCAST} plan markers, is a <em>broadcast consumer</em>.
     * </dd>
     * </dl>
     * <!-- CHECKSTYLE:ON JavadocStyle -->
     *
     * <p>
     * The specified operator graph must satisfy following preconditions:
     * </p>
     * <ol>
     * <li> each operator except {@code BEGIN} plan marker must have at least one predecessors </li>
     * <li> each operator except {@code END} plan marker must have at least one successors </li>
     * <li> each {@code GATHER} plan marker must have just one successor </li>
     * <li>
     *   each nearest forward plan marker of <em>gathering operator</em>,
     *   must be either a {@code GATHER} or {@code BROADCAST} plan marker
     * </li>
     * <li>
     *   each plan marker must not be a <em>broadcast consumer</em>
     * </li>
     * </ol>
     *
     * <p>
     * The created plan will work as equivalent to the original operator graph,
     * and each sub-plan will have the following properties:
     * </p>
     * <ol>
     * <li>
     *   each sub-plan has minimal inputs within the following constraints:
     *   <ol>
     *   <li>
     *     each sub-plan must have at least one input other than {@code BROADCAST} plan markers
     *   </li>
     *   <li>
     *     if a sub-plan has a <em>gathering operator</em>,
     *     the sub-plan inputs must contain the all forward reachable plan markers from it
     *   </li>
     *   <li>
     *     if a sub-plan has a <em>broadcasting consumer</em>,
     *     the sub-plan inputs must contain the related all {@code BROADCAST} plan markers
     *   </li>
     *   </ol>
     * </li>
     * <li>
     *   each sub-plan just has one output
     * </li>
     * <li>
     *   each sub-plan has minimal operators within the following constraints:
     *   <ol>
     *   <li> each operator except sub-plan input must be reachable to at least one inputs of the sub-plan </li>
     *   <li> each operator except sub-plan output must be reachable to at least one outputs of the sub-plan </li>
     *   <li> each operator except sub-plan input and output must be not a plan marker </li>
     *   </ol>
     * </li>
     * </ol>
     *
     * <p>
     * To keep the original semantics, the plan will have the following properties:
     * </p>
     * <ol>
     * <li>
     *   each sub-plan consists of all combinations of possible inputs and output pairs
     * </li>
     * <li>
     *   each sub-plan has the unique inputs and output pairs
     * </li>
     * </ol>
     * @param graph the target operator graph
     * @return the organized primitive plan
     * @see #normalize(OperatorGraph)
     * @see #simplifyTerminators(OperatorGraph)
     * @see #startAssemblePlan(PlanDetail)
     */
    public static PlanDetail createPrimitivePlan(OperatorGraph graph) {
        return PrimitivePlanner.plan(graph.getOperators());
    }

    /**
     * Creates a new plan assembler for the target plan.
     * @param detail detail of assemble target plan.
     * @return a new empty assembler for the target plan
     */
    public static PlanAssembler startAssemblePlan(PlanDetail detail) {
        return new PlanAssembler(detail);
    }

    /**
     * Returns a sub-plan dependency graph for the specified plan.
     * @param plan the target plan
     * @return a sub-plan dependency graph ({@code sub-plan -> its predecessors})
     */
    public static Graph<SubPlan> toDependencyGraph(Plan plan) {
        Graph<SubPlan> results = Graphs.newInstance();
        for (SubPlan element : plan.getElements()) {
            results.addNode(element);
            for (SubPlan.Input downstream : element.getInputs()) {
                for (SubPlan.Output upstream : downstream.getOpposites()) {
                    results.addEdge(element, upstream.getOwner());
                }
            }
        }
        return results;
    }

    /**
     * Returns an operator dependency graph for the specified sub-plan.
     * @param subPlan the target sub-plan
     * @return a sub-plan dependency graph ({@code operator -> its predecessors})
     */
    public static Graph<Operator> toDependencyGraph(SubPlan subPlan) {
        Graph<Operator> results = Graphs.newInstance();
        for (Operator element : subPlan.getOperators()) {
            results.addNode(element);
            for (OperatorInput downstream : element.getInputs()) {
                for (OperatorOutput upstream : downstream.getOpposites()) {
                    results.addEdge(element, upstream.getOwner());
                }
            }
        }
        return results;
    }
}
