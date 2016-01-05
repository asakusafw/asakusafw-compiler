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
package com.asakusafw.lang.compiler.planning.basic;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * A basic implementation of {@link Plan}.
 */
public class BasicPlan extends BasicAttributeContainer implements Plan {

    private static final int MAX_STABLE_SORT = 1000;

    private final Set<BasicSubPlan> elements = new LinkedHashSet<>();

    @Override
    public Set<BasicSubPlan> getElements() {
        return new LinkedHashSet<>(elements);
    }

    /**
     * Adds a new sub-plan to this.
     * The specified operators must satisfy following preconditions:
     * <ol>
     * <li> each input operator must be a plan marker </li>
     * <li> each input operator must not have any predecessors </li>
     * <li> each input operator must be forward reachable to at least one output operator </li>
     * <li> each output operator must be a plan marker </li>
     * <li> each output operator must not have any successors </li>
     * <li> each output operator must be backward reachable to at least one input operator </li>
     * </ol>
     *
     * The created sub-plan will have following properties:
     * <ol>
     * <li> the sub-plan satisfies {@link SubPlan the common invariants of sub-plan} </li>
     * <li> each input operator becomes as its {@link SubPlan#getInputs() sub-plan input} </li>
     * <li> each output operator becomes as its {@link SubPlan#getOutputs() sub-plan output} </li>
     * <li> {@link SubPlan#getOwner()} returns this {@link BasicPlan} </li>
     * <li> {@link SubPlan#getOperators()} returns union of the input, output, and body operators </li>
     * <li> each sub-plan input does not have any opposites </li>
     * <li> each sub-plan outputs does not have any opposites </li>
     * </ol>
     *
     * Clients should not modify any operators in the sub-plan.
     * @param inputs the sub-plan input operators
     * @param outputs the sub-plan output operators
     * @return the created sub-plan
     * @see PlanMarker
     * @see PlanMarkers
     */
    public BasicSubPlan addElement(Set<? extends MarkerOperator> inputs, Set<? extends MarkerOperator> outputs) {
        BasicSubPlan element = new BasicSubPlan(this, inputs, outputs);
        elements.add(element);
        return element;
    }

    /**
     * Removes the sub-plan from this plan.
     * @param sub the sub-plan
     */
    public void removeElement(BasicSubPlan sub) {
        if (sub.getOwner() != this) {
            throw new IllegalArgumentException();
        }
        for (BasicSubPlan.BasicInput port : sub.getInputs()) {
            port.disconnectAll();
        }
        for (BasicSubPlan.BasicOutput port : sub.getOutputs()) {
            port.disconnectAll();
        }
        elements.remove(sub);
    }

    /**
     * Sorts the sub-plans by their topological structure.
     */
    public void sort() {
        boolean stable = elements.size() <= MAX_STABLE_SORT;
        sort(stable);
    }

    /**
     * Sorts the sub-plans by their topological structure.
     * @param stable {@code true} to use stable sort
     */
    public void sort(boolean stable) {
        List<BasicSubPlan> newElements;
        if (stable) {
            newElements = sortStable();
        } else {
            newElements = Graphs.sortPostOrder(buildDependencyGraph());
        }
        assert elements.size() == newElements.size();
        elements.clear();
        elements.addAll(newElements);
    }

    private Graph<BasicSubPlan> buildDependencyGraph() {
        Graph<BasicSubPlan> results = Graphs.newInstance();
        for (BasicSubPlan element : elements) {
            results.addNode(element);
            for (BasicSubPlan.BasicInput downstream : element.getInputs()) {
                for (BasicSubPlan.BasicOutput upstream : downstream.getOpposites()) {
                    results.addEdge(element, upstream.getOwner());
                }
            }
        }
        Set<Set<BasicSubPlan>> circuits = Graphs.findCircuit(results);
        if (circuits.isEmpty() == false) {
            List<Diagnostic> diagnostics = new ArrayList<>();
            for (Set<BasicSubPlan> loop : circuits) {
                diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                        "plan must by acyclic: {0}",
                        loop)));
            }
            throw new DiagnosticException(diagnostics);
        }
        return results;
    }

    private List<BasicSubPlan> sortStable() {
        Graph<BasicSubPlan> graph = buildDependencyGraph();
        List<BasicSubPlan> results = new ArrayList<>();
        LinkedList<BasicSubPlan> work = new LinkedList<>(elements);
        while (work.isEmpty() == false) {
            boolean changed = false;
            for (Iterator<BasicSubPlan> iter = work.iterator(); iter.hasNext();) {
                BasicSubPlan next = iter.next();
                Set<BasicSubPlan> blockers = graph.getConnected(next);
                if (blockers.isEmpty()) {
                    iter.remove();
                    graph.removeNode(next);
                    results.add(next);
                    changed = true;
                    break;
                }
            }
            if (changed == false) {
                throw new AssertionError("internal error: not acyclic plan"); //$NON-NLS-1$
            }
        }
        assert elements.size() == results.size();
        return results;
    }

    @Override
    public String toString() {
        return String.format(
                "Plan(%08x)", //$NON-NLS-1$
                hashCode());
    }
}
