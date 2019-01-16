/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * A basic implementation of {@link SubPlan}.
 */
public final class BasicSubPlan extends BasicAttributeContainer implements SubPlan {

    private final BasicPlan owner;

    private final Set<Operator> operators;

    private final Map<MarkerOperator, BasicInput> inputs;

    private final Map<MarkerOperator, BasicOutput> outputs;

    BasicSubPlan(
            BasicPlan owner,
            Set<? extends MarkerOperator> inputs,
            Set<? extends MarkerOperator> outputs) {
        this.owner = owner;
        this.inputs = buildInputs(inputs);
        this.outputs = buildOutputs(outputs);
        this.operators = collectOperators(inputs, outputs);
        validate();
    }

    private Map<MarkerOperator, BasicInput> buildInputs(Set<? extends MarkerOperator> markers) {
        Map<MarkerOperator, BasicInput> results = new LinkedHashMap<>();
        for (MarkerOperator operator : markers) {
            PlanMarker marker = PlanMarkers.get(operator);
            if (marker == null) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "sub-plan input must be a plan marker: {0}", //$NON-NLS-1$
                        operator));
            }
            if (Operators.hasPredecessors(operator)) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "sub-plan input must not have any predecessors: {0}", //$NON-NLS-1$
                        operator));
            }
            if (Operators.hasSuccessors(operator) == false) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "sub-plan input must have at least one successor: {0}", //$NON-NLS-1$
                        operator));
            }
            results.put(operator, new BasicInput(operator));
        }
        return results;
    }

    private Map<MarkerOperator, BasicOutput> buildOutputs(Set<? extends MarkerOperator> markers) {
        Map<MarkerOperator, BasicOutput> results = new LinkedHashMap<>();
        for (MarkerOperator operator : markers) {
            PlanMarker marker = PlanMarkers.get(operator);
            if (marker == null) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "sub-plan output must be a plan marker: {0}", //$NON-NLS-1$
                        operator));
            }
            if (Operators.hasSuccessors(operator)) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "sub-plan output must not have any successors: {0}", //$NON-NLS-1$
                        operator));
            }
            if (Operators.hasPredecessors(operator) == false) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "sub-plan output must have at least one predecessor: {0}", //$NON-NLS-1$
                        operator));
            }
            results.put(operator, new BasicOutput(operator));
        }
        return results;
    }

    private static Set<Operator> collectOperators(Set<? extends Operator> inputs, Set<? extends Operator> outputs) {
        Set<Operator> base = new HashSet<>();
        base.addAll(inputs);
        base.addAll(outputs);
        return Operators.getTransitiveConnected(base);
    }

    private void validate() {
        /* NOTE: it is too strict
        for (Operator operator : operators) {
            if (PlanMarkers.get(operator) != null) {
                if (inputs.containsKey(operator) == false && outputs.containsKey(operator) == false) {
                    throw new IllegalStateException(MessageFormat.format(
                            "plan marker must be either a sub-plan input or output: {0}",
                            operator));
                }
            }
        }
        */
        Set<Operator> effectives = computeEffectiveOperators();
        if (operators.equals(effectives) == false) {
            assert operators.equals(Operators.getTransitiveConnected(operators));
            assert operators.containsAll(effectives);
            Set<Operator> redundant = new HashSet<>();
            redundant.addAll(operators);
            redundant.removeAll(effectives);
            throw new IllegalStateException(MessageFormat.format(
                    "sub-plan includes redundant operators: {0}", //$NON-NLS-1$
                    redundant));
        }
    }

    private Set<Operator> computeEffectiveOperators() {
        Set<Operator> inputOperators = new HashSet<>();
        for (Input port : inputs.values()) {
            inputOperators.add(port.getOperator());
        }
        Set<Operator> inputReachables = new HashSet<>();
        inputReachables.addAll(inputOperators);
        inputReachables.addAll(Operators.getTransitiveSuccessors(Operators.getOutputs(inputOperators)));

        Set<Operator> outputOperators = new HashSet<>();
        for (Output port : outputs.values()) {
            outputOperators.add(port.getOperator());
        }
        Set<Operator> outputReachables = new HashSet<>();
        outputReachables.addAll(outputOperators);
        outputReachables.addAll(Operators.getTransitivePredecessors(Operators.getInputs(outputOperators)));

        // reachable from inputs/outputs
        Set<Operator> effective = new HashSet<>();
        effective.addAll(inputReachables);
        effective.retainAll(outputReachables);
        return effective;
    }

    @Override
    public BasicPlan getOwner() {
        return owner;
    }

    @Override
    public Set<Operator> getOperators() {
        return operators;
    }

    @Override
    public Set<BasicInput> getInputs() {
        return new LinkedHashSet<>(inputs.values());
    }

    @Override
    public Set<BasicOutput> getOutputs() {
        return new LinkedHashSet<>(outputs.values());
    }

    @Override
    public BasicInput findInput(Operator operator) {
        return inputs.get(operator);
    }

    @Override
    public BasicOutput findOutput(Operator operator) {
        return outputs.get(operator);
    }

    /**
     * Removes an operator from this sub-plan.
     * @param operator the operator
     */
    public void removeOperator(Operator operator) {
        if (operators.contains(operator)) {
            operator.disconnectAll();
            operators.remove(operator);
            if (inputs.containsKey(operator)) {
                inputs.remove(operator).disconnectAll();
            }
            if (outputs.containsKey(operator)) {
                outputs.remove(operator).disconnectAll();
            }
        }
    }

    @Override
    public String toString() {
        return String.format(
                "SubPlan(%08x)@%s", //$NON-NLS-1$
                hashCode(),
                owner);
    }

    /**
     * An abstract implementation of {@link com.asakusafw.lang.compiler.planning.SubPlan.Port}.
     * @param <TSelf> the self type
     * @param <TOpposite> the opposite port type
     */
    public abstract static class BasicPort<
                TSelf extends BasicPort<TSelf, TOpposite>,
                TOpposite extends BasicPort<TOpposite, TSelf>>
            extends BasicAttributeContainer implements SubPlan.Port {

        private final BasicSubPlan owner;

        private final MarkerOperator operator;

        private final Set<TOpposite> opposites = new LinkedHashSet<>();

        BasicPort(BasicSubPlan owner, MarkerOperator operator) {
            this.owner = owner;
            this.operator = operator;
        }

        abstract TSelf getSelf();

        @Override
        public BasicSubPlan getOwner() {
            return owner;
        }

        @Override
        public MarkerOperator getOperator() {
            return operator;
        }

        /**
         * Connects to the opposite port.
         * If this has been already connected to the opposite, this does not nothing.
         * @param opposite the opposite port
         */
        public void connect(TOpposite opposite) {
            this.connect0(opposite);
            opposite.connect0(getSelf());
        }

        /**
         * Disconnects from the opposite port.
         * If this does not have not been connected to the opposite yet, this does not nothing.
         * @param opposite the opposite port
         */
        public void disconnect(TOpposite opposite) {
            this.disconnect0(opposite);
            opposite.disconnect0(getSelf());
        }

        /**
         * Disconnects from the all opposite ports.
         */
        public void disconnectAll() {
            for (TOpposite opposite : opposites) {
                opposite.disconnect0(getSelf());
            }
            opposites.clear();
        }

        void connect0(TOpposite opposite) {
            opposites.add(opposite);
        }

        void disconnect0(TOpposite opposite) {
            opposites.remove(opposite);
        }

        @Override
        public Set<TOpposite> getOpposites() {
            return new LinkedHashSet<>(opposites);
        }
    }

    /**
     * A basic implementation of {@link com.asakusafw.lang.compiler.planning.SubPlan.Input}.
     */
    public class BasicInput extends BasicPort<BasicInput, BasicOutput> implements Input {

        /**
         * Creates a new instance.
         * @param operator the original operator
         */
        public BasicInput(MarkerOperator operator) {
            super(BasicSubPlan.this, operator);
        }

        @Override
        BasicInput getSelf() {
            return this;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "Input({0})@{1}", //$NON-NLS-1$
                    getOperator(),
                    getOwner());
        }
    }

    /**
     * A basic implementation of {@link com.asakusafw.lang.compiler.planning.SubPlan.Output}.
     */
    public class BasicOutput extends BasicPort<BasicOutput, BasicInput> implements Output {

        /**
         * Creates a new instance.
         * @param operator the original operator
         */
        public BasicOutput(MarkerOperator operator) {
            super(BasicSubPlan.this, operator);
        }

        @Override
        BasicOutput getSelf() {
            return this;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "Output({0})@{1}", //$NON-NLS-1$
                    getOperator(),
                    getOwner());
        }
    }
}
