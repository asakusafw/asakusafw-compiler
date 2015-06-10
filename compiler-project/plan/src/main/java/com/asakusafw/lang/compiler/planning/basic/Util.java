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
package com.asakusafw.lang.compiler.planning.basic;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.common.Predicate;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;

final class Util {

    private Util() {
        return;
    }

    public static Predicate<Operator> isMember(Set<? extends Operator> operators) {
        return new IsMember(operators);
    }

    public static Set<OperatorInput> computeBroadcastInputs(
            Collection<? extends Operator> inputs,
            Collection<? extends Operator> outputs) {
        Set<Operator> consumers = new HashSet<>(computeBroadcastConsumers(inputs, outputs).keySet());
        Set<Operator> terminators = new HashSet<>();
        terminators.addAll(inputs);
        terminators.addAll(consumers);

        Predicate<Operator> isTerminator = new IsMember(terminators);
        Set<OperatorInput> results = new HashSet<>();
        for (Operator consumer : consumers) {
            for (OperatorInput port : consumer.getInputs()) {
                Set<Operator> sources = Operators.findNearestReachablePredecessors(
                        Collections.singleton(port),
                        isTerminator);
                if (sources.isEmpty()) {
                    continue;
                }
                boolean sawBroadcast = false;
                boolean sawNonBroadcast = false;
                for (Operator source : sources) {
                    PlanMarker marker = PlanMarkers.get(source);
                    if (marker == PlanMarker.BROADCAST) {
                        sawBroadcast = true;
                    } else {
                        sawNonBroadcast = true;
                    }
                }
                // invalid broadcast input
                if (sawBroadcast && sawNonBroadcast) {
                    throw new IllegalStateException(MessageFormat.format(
                            "invalid broadcast input: {2} -> {0}#{1}", //$NON-NLS-1$
                            consumer,
                            port.getName(),
                            sources));
                } else if (sawBroadcast) {
                    results.add(port);
                }
            }
        }
        return results;
    }

    public static Map<Operator, Set<MarkerOperator>> computeBroadcastConsumers(
            Collection<? extends Operator> inputs,
            Collection<? extends Operator> outputs) {
        Set<Operator> in = new HashSet<>(inputs);
        Set<Operator> out = new HashSet<>(outputs);
        return computeBroadcastConsumers0(in, out);
    }

    private static Map<Operator, Set<MarkerOperator>> computeBroadcastConsumers0(
            Set<Operator> inputs, Set<Operator> outputs) {
        Set<Operator> nonBroadcastOperators = collectNonBroadcastOperators(inputs, outputs);
        Predicate<Operator> isNonBroadcast = new IsMember(nonBroadcastOperators);
        Map<Operator, Set<MarkerOperator>> results = new HashMap<>();
        for (Operator operator : inputs) {
            PlanMarker kind = PlanMarkers.get(operator);
            assert kind != null : operator;
            if (kind != PlanMarker.BROADCAST) {
                continue;
            }
            MarkerOperator source = (MarkerOperator) operator;
            // search for nearest non-broadcast operators from each BROADCAST plan marker:
            // it will consumes this broadcast data directly
            Set<Operator> consumers = Operators.findNearestReachableSuccessors(
                    source.getOutputs(), isNonBroadcast);
            for (Operator consumer : consumers) {
                Set<MarkerOperator> targets = results.get(consumer);
                if (targets == null) {
                    targets = new HashSet<>();
                    results.put(consumer, targets);
                }
                targets.add(source);
            }
        }
        return results;
    }


    private static Set<Operator> collectNonBroadcastOperators(Set<Operator> inputs, Set<Operator> outputs) {
        Set<OperatorOutput> nonBroadcastOutputs = new HashSet<>();
        for (Operator operator : inputs) {
            PlanMarker kind = PlanMarkers.get(operator);
            assert kind != null : operator;
            if (kind == PlanMarker.BROADCAST) {
                continue;
            }
            nonBroadcastOutputs.addAll(operator.getOutputs());
        }
        // non-broadcast operator is:
        Set<Operator> results = new HashSet<>();
        // - each outputs
        results.addAll(outputs);
        // - each operator which nearest forward reachable plan markers contain other than BROADCAST
        results.addAll(Operators.collectUntilNearestReachableSuccessors(
                nonBroadcastOutputs,
                new IsMember(outputs),
                false));

        return results;
    }

    private static final class IsMember implements Predicate<Operator> {

        private final Set<? extends Operator> members;

        public IsMember(Set<? extends Operator> operators) {
            this.members = operators;
        }

        @Override
        public boolean apply(Operator argument) {
            return members.contains(argument);
        }
    }
}
