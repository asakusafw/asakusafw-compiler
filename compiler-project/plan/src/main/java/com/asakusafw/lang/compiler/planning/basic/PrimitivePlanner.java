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
package com.asakusafw.lang.compiler.planning.basic;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.planning.PlanBuilder;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.Planning;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Generates a primitive plan.
 * @see Planning#createPrimitivePlan(com.asakusafw.lang.compiler.model.graph.OperatorGraph)
 */
public final class PrimitivePlanner {

    private final Set<Operator> operators;

    private final Set<MarkerOperator> planMarkers;

    private PrimitivePlanner(Set<Operator> operators) {
        this.operators = operators;
        this.planMarkers = collectPlanMarkers(operators);
    }

    /**
     * Returns all plan markers in the operator.
     * Validates successors and predecessors of each operator.
     * @param operators the source operators
     * @return the plan markers
     */
    private static Set<MarkerOperator> collectPlanMarkers(Set<Operator> operators) {
        Set<MarkerOperator> results = new HashSet<>();
        for (Operator operator : operators) {
            PlanMarker marker = PlanMarkers.get(operator);
            if (marker != PlanMarker.BEGIN) {
                if (Operators.hasPredecessors(operator) == false) {
                    throw new IllegalArgumentException(MessageFormat.format(
                            "operator except BEGIN plan marker must have at least one predecessors: {0}", //$NON-NLS-1$
                            operator));
                }
            }
            if (marker != PlanMarker.END) {
                if (Operators.hasSuccessors(operator) == false) {
                    throw new IllegalArgumentException(MessageFormat.format(
                            "operator except END plan marker must have at least one succesors: {0}", //$NON-NLS-1$
                            operator));
                }
            }
            if (marker != null) {
                results.add((MarkerOperator) operator);
            }
        }
        return results;
    }

    /**
     * Returns a detail of the primitive plan.
     * @param operators the target operators
     * @return the created primitive plan
     */
    public static PlanDetail plan(Collection<? extends Operator> operators) {
        return new PrimitivePlanner(Operators.getTransitiveConnected(operators)).plan();
    }

    private PlanDetail plan() {
        validateGraph();
        Set<Set<MarkerOperator>> inputCandidates = collectInputCandidates();
        Map<Operator, Set<MarkerOperator>> broadcastConsumers = collectBroadcastConsumers();
        PlanBuilder builder = PlanBuilder.from(operators)
                .withSort(true);

        for (Set<MarkerOperator> inputCandidate : inputCandidates) {
            // detects output candidates for the input group
            // (in here, the input group does not include any BROADCAST plan markers)
            Set<MarkerOperator> outputCandidates = collectPossibleOutputOperatorsFor(inputCandidate);
            for (MarkerOperator output : outputCandidates) {
                // detects related broadcast inputs from (input*, output*)
                Set<MarkerOperator> broadcasts = collectBroadcastsFor(
                        inputCandidate,
                        Collections.singleton(output),
                        broadcastConsumers);
                Set<MarkerOperator> finalInput;
                if (broadcasts.isEmpty()) {
                    finalInput = inputCandidate;
                } else {
                    finalInput = new HashSet<>();
                    finalInput.addAll(inputCandidate);
                    finalInput.addAll(broadcasts);
                }
                assert finalInput.contains(output) == false;
                // then builds a new primitive sub-plan
                builder.add(finalInput, Collections.singleton(output));
            }
        }
        return builder.build();
    }

    private void validateGraph() {
        Graph<Operator> g = Graphs.newInstance();
        for (Operator operator : operators) {
            g.addNode(operator);
            g.addEdges(operator, Operators.getPredecessors(operator));
        }
        Set<Set<Operator>> circuits = Graphs.findCircuit(g);
        if (circuits.isEmpty() == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "operator graph must not have any circuits: {0}", //$NON-NLS-1$
                    circuits));
        }
    }

    /**
     * Collects sub-plan input candidates.
     * Candidates do not include any BROADCAST plan markers.
     * Validates a constraint about GATHER.
     * @return the sub-plan input groups
     */
    private Set<Set<MarkerOperator>> collectInputCandidates() {
        Set<MarkerOperator> excepts = new HashSet<>();
        Set<Set<MarkerOperator>> results = new HashSet<>();
        // first, we collect GATHER, BROADCASTS, and END
        for (MarkerOperator marker : planMarkers) {
            PlanMarker kind = PlanMarkers.get(marker);
            if (kind == PlanMarker.END && Operators.hasSuccessors(marker) == false) {
                // END can not become sub-plan inputs
                excepts.add(marker);
                continue;
            }
            if (kind == PlanMarker.BROADCAST) {
                // BROADCAST can not become sub-plan inputs directly
                excepts.add(marker);
                continue;
            }
            if (kind == PlanMarker.GATHER) {
                // GATHER: first, collects neighbors
                Set<MarkerOperator> gatheringInputGroup = collectGatheringInputGroup(marker);
                assert gatheringInputGroup.contains(marker);
                // adds a group as sub-plan inputs,
                // and remove individual plan markers from input candidates
                results.add(gatheringInputGroup);
                excepts.addAll(gatheringInputGroup);
                continue;
            }
        }
        // then, we collect the rest plan markers as independent sub-plan inputs
        for (MarkerOperator marker : planMarkers) {
            if (excepts.contains(marker) == false) {
                results.add(Collections.singleton(marker));
            }
        }
        return results;
    }

    private Set<MarkerOperator> collectGatheringInputGroup(MarkerOperator gather) {
        assert PlanMarkers.get(gather) == PlanMarker.GATHER;
        Set<Operator> successors = Operators.getSuccessors(gather);
        // we already know that plan marker except END has >= 1 successors
        assert successors.isEmpty() == false;
        if (successors.size() >= 2) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "GATHER plan marker must have just one successor: {0}", //$NON-NLS-1$
                    gather));
        }
        // detects gathering input group
        Operator gathering = successors.iterator().next();
        Set<Operator> gatherGroupCandidates = Operators.findNearestReachablePredecessors(
                gathering.getInputs(),
                PlanMarkers::exists);
        assert gatherGroupCandidates.contains(gather);
        Set<MarkerOperator> results = new HashSet<>();
        for (Operator operator : gatherGroupCandidates) {
            PlanMarker kind = PlanMarkers.get(operator);
            assert kind != null : operator;
            if (kind == PlanMarker.BROADCAST) {
                // BROADCAST can not become sub-plan inputs directly
                continue;
            }
            if (kind != PlanMarker.GATHER) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "gathering operator requires inputs as GATHER/BROADCAST: {1} -> {0}", //$NON-NLS-1$
                        gathering,
                        operator));
            }
            results.add((MarkerOperator) operator);
        }
        return results;
    }

    private Map<Operator, Set<MarkerOperator>> collectBroadcastConsumers() {
        Map<Operator, Set<MarkerOperator>> results = Util.computeBroadcastConsumers(planMarkers, planMarkers);
        for (Map.Entry<Operator, Set<MarkerOperator>> entry : results.entrySet()) {
            Operator consumer = entry.getKey();
            if (planMarkers.contains(consumer)) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "plan marker must not be a broadcast consumer: {0} -> {1}", //$NON-NLS-1$
                        entry.getValue(),
                        consumer));
            }
        }
        return results;
    }

    /**
     * Returns possible output operators for the input operators.
     * @param inputs the input operators
     * @return the possible output operators
     */
    private Set<MarkerOperator> collectPossibleOutputOperatorsFor(Set<MarkerOperator> inputs) {
        Set<MarkerOperator> outputCandidates = toMarkers(Operators.findNearestReachableSuccessors(
                Operators.getOutputs(inputs),
                PlanMarkers::exists));
        return outputCandidates;
    }

    /**
     * Returns required BROADCAST plan markers for the (input*, output) pair.
     * @param inputs the input operators
     * @param outputs the output operators
     * @param broadcastConsumers the all broadcast consumers and their requiring BROADCAST plan markers
     * @return the required BROADCAST plan markers
     */
    private Set<MarkerOperator> collectBroadcastsFor(
            Set<? extends Operator> inputs,
            Set<? extends Operator> outputs,
            Map<Operator, Set<MarkerOperator>> broadcastConsumers) {
        if (broadcastConsumers.isEmpty()) {
            return Collections.emptySet();
        }
        // detects body operators from inputs and output
        Set<Operator> bodyOperators = new HashSet<>();
        bodyOperators.addAll(Operators.collectUntilNearestReachableSuccessors(
                Operators.getOutputs(inputs),
                PlanMarkers::exists,
                false));
        bodyOperators.retainAll(Operators.collectUntilNearestReachablePredecessors(
                Operators.getInputs(outputs),
                PlanMarkers::exists,
                false));

        // find for broadcast consumers, and returns the corresponded BROADCAST plan markers
        Set<MarkerOperator> results = new HashSet<>();
        for (Operator operator : bodyOperators) {
            Set<MarkerOperator> broadcasts = broadcastConsumers.get(operator);
            if (broadcasts != null) {
                results.addAll(broadcasts);
            }
        }
        return results;
    }

    private static Set<MarkerOperator> toMarkers(Collection<? extends Operator> operators) {
        Set<MarkerOperator> results = new HashSet<>();
        for (Operator operator : operators) {
            assert PlanMarkers.get(operator) != null;
            results.add((MarkerOperator) operator);
        }
        return results;
    }
}
