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
package com.asakusafw.lang.compiler.planning.basic;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.compiler.planning.basic.BasicSubPlan.BasicInput;

/**
 * A group of isomorphic operators.
 */
final class OperatorGroup {

    private final BasicSubPlan owner;

    private final Set<Operator> operators;

    private final BitSet broadcastInputIndices;

    private final Set<Attribute> attributes;

    OperatorGroup(
            BasicSubPlan owner, Set<Operator> operators,
            BitSet broadcastInputIndices, Set<Attribute> attributes) {
        this.owner = owner;
        this.operators = new LinkedHashSet<>(operators);
        this.broadcastInputIndices = (BitSet) broadcastInputIndices.clone();
        this.attributes = EnumUtil.freeze(attributes);
    }

    public static Set<Attribute> getAttributes(SubPlan owner, Operator operator) {
        Set<Attribute> results = EnumSet.noneOf(Attribute.class);
        if (isExtractKind(owner, operator)) {
            results.add(Attribute.EXTRACT_KIND);
        }
        if (operator.getConstraints().contains(OperatorConstraint.GENERATOR)) {
            results.add(Attribute.GENERATOR);
        }
        if (operator.getConstraints().contains(OperatorConstraint.AT_LEAST_ONCE)) {
            results.add(Attribute.CONSUMER);
        }
        if (owner.findInput(operator) != null) {
            results.add(Attribute.INPUT);
        }
        if (owner.findOutput(operator) != null) {
            results.add(Attribute.OUTPUT);
        }
        PlanMarker marker = PlanMarkers.get(operator);
        if (marker != null) {
            switch (marker) {
            case BEGIN:
                results.add(Attribute.BEGIN);
                break;
            case END:
                results.add(Attribute.END);
                break;
            case CHECKPOINT:
                results.add(Attribute.CHECKPOINT);
                break;
            case GATHER:
                results.add(Attribute.GATHER);
                break;
            case BROADCAST:
                results.add(Attribute.BROADCAST);
                break;
            default:
                // ignore unknown markers
                break;
            }
        }
        return results;
    }

    private static boolean isExtractKind(SubPlan owner, Operator operator) {
        // for safety, we assumes that GATHER (input) is not extract kind
        if (PlanMarkers.get(operator) == PlanMarker.GATHER
                && owner.findInput(operator) != null) {
            return false;
        }
        // operator following GATHER is not extract kind
        for (Operator pred : Operators.getPredecessors(operator)) {
            if (PlanMarkers.get(pred) == PlanMarker.GATHER) {
                return false;
            }
        }
        return true;
    }

    public boolean isEmpty() {
        return operators.isEmpty();
    }

    public boolean has(Attribute attribute) {
        return attributes.contains(attribute);
    }

    public void remove(Operator operator) {
        if (operators.remove(operator)) {
            owner.removeOperator(operator);
        }
    }

    public boolean applyRedundantOutputElimination() {
        return applyCombination((base, target) -> applyRedundantOutputElimination(base, target));
    }

    boolean applyRedundantOutputElimination(Operator base, Operator target) {
        // a +-- $ --- b
        //    \- $ --- c
        // >>>
        // a --- $ +-- b
        //    \- $  \- c
        if (hasSameInputs(base, target) == false) {
            return false;
        }
        if (attributes.contains(Attribute.OUTPUT)) {
            BasicSubPlan.BasicOutput baseOut = owner.findOutput(base);
            BasicSubPlan.BasicOutput targetOut = owner.findOutput(target);
            assert baseOut != null;
            assert targetOut != null;
            Set<BasicInput> downstreams = targetOut.getOpposites();
            if (downstreams.isEmpty()) {
                return false;
            }
            for (BasicSubPlan.BasicInput downstream : downstreams) {
                baseOut.connect(downstream);
            }
            targetOut.disconnectAll();
            return true;
        } else {
            if (Operators.hasSuccessors(target) == false) {
                return false;
            }
            List<OperatorOutput> sources = target.getOutputs();
            List<OperatorOutput> destinations = base.getOutputs();
            assert sources.size() == destinations.size();
            for (int i = 0, n = sources.size(); i < n; i++) {
                OperatorOutput source = sources.get(i);
                OperatorOutput destination = destinations.get(i);
                assert source.getDataType().equals(destination.getDataType());
                Collection<OperatorInput> downstreams = source.getOpposites();
                if (downstreams.isEmpty() == false) {
                    Operators.connectAll(destination, downstreams);
                    source.disconnectAll();
                }
            }
            return true;
        }
    }

    public boolean applyUnionPushDown() {
        if (attributes.contains(Attribute.EXTRACT_KIND) == false
                || attributes.contains(Attribute.GENERATOR)) {
            return false;
        }
        return applyCombination((base, target) -> applyUnionPushDown(base, target));
    }

    boolean applyUnionPushDown(Operator base, Operator target) {
        if (hasSameOutputs(base, target) == false || hasSameBroadcastInputs(base, target) == false) {
            return false;
        }
        // a --- $ --+ c
        // b --- $ -/
        // >>>
        // a --+ $ --+ c
        // b -/  $ -/
        if (attributes.contains(Attribute.INPUT)) {
            BasicSubPlan.BasicInput baseIn = owner.findInput(base);
            BasicSubPlan.BasicInput targetIn = owner.findInput(target);
            assert baseIn != null;
            assert targetIn != null;
            Set<BasicSubPlan.BasicOutput> upstreams = targetIn.getOpposites();
            if (upstreams.isEmpty()) {
                return false;
            }
            for (BasicSubPlan.BasicOutput upstream : upstreams) {
                baseIn.connect(upstream);
            }
            targetIn.disconnectAll();
            return true;
        } else {
            if (Operators.hasPredecessors(target) == false) {
                return false;
            }
            List<OperatorInput> sources = target.getInputs();
            List<OperatorInput> destinations = base.getInputs();
            assert sources.size() == destinations.size();
            for (int i = 0, n = sources.size(); i < n; i++) {
                OperatorInput source = sources.get(i);
                if (broadcastInputIndices.get(i) == false) {
                    OperatorInput destination = destinations.get(i);
                    assert source.getDataType().equals(destination.getDataType());
                    Collection<OperatorOutput> upstreams = source.getOpposites();
                    Operators.connectAll(upstreams, destination);
                }
                source.disconnectAll();
            }
            return true;
        }
    }

    private boolean applyCombination(Applier applier) {
        if (operators.size() <= 1) {
            return false;
        }
        boolean changed = false;
        BitSet applied = new BitSet();
        Operator[] ops = operators.toArray(new Operator[operators.size()]);
        for (int baseIndex = 0; baseIndex < ops.length - 1; baseIndex++) {
            if (applied.get(baseIndex)) {
                continue;
            }
            Operator base = ops[baseIndex];
            for (int targetIndex = baseIndex + 1; targetIndex < ops.length; targetIndex++) {
                if (applied.get(targetIndex)) {
                    continue;
                }
                Operator target = ops[targetIndex];
                if (applier.apply(base, target)) {
                    applied.set(targetIndex);
                    changed = true;
                }
            }
        }
        return changed;
    }

    public boolean applyTrivialOutputElimination() {
        if (operators.isEmpty()) {
            return false;
        }
        // NOTE this never change sub-plan inputs/outputs
        // non-extract/generator/consumer is checked later
        if (attributes.contains(Attribute.INPUT)
                || attributes.contains(Attribute.OUTPUT)) {
            return false;
        }
        boolean changed = false;
        for (Operator operator : operators) {
            changed |= applyTrivialOutputElimination(operator);
        }
        return changed;
    }

    private boolean applyTrivialOutputElimination(Operator operator) {
        if (Operators.hasSuccessors(operator) == false) {
            return false;
        }
        // () ===> a --- $ --- b
        // >>>
        // () ===> a +-------- b
        //            \- $
        boolean changed = false;

        // shrink trivial upstream operators
        // but here we keep at least one upstream per port
        List<OperatorInput> inputs = operator.getInputs();
        for (int i = 0, n = inputs.size(); i < n; i++) {
            changed |= shrinkTrivialUpstreams(inputs.get(i));
        }

        // never remove non-extract/generator/consumer operators
        if (attributes.contains(Attribute.EXTRACT_KIND) == false
                || attributes.contains(Attribute.GENERATOR)
                || attributes.contains(Attribute.CONSUMER)) {
            return changed;
        }

        // try remove operator itself
        MarkerOperator primary = null;
        for (int i = 0, n = inputs.size(); i < n; i++) {
            // ignores broadcast inputs
            if (broadcastInputIndices.get(i)) {
                continue;
            }
            OperatorInput input = inputs.get(i);
            for (OperatorOutput upstream : input.getOpposites()) {
                SubPlan.Input in = owner.findInput(upstream.getOwner());
                if (in != null && in.getOpposites().isEmpty()) {
                    if (primary == null) {
                        primary = in.getOperator();
                    }
                } else {
                    // operator is not empty
                    return changed;
                }
            }
        }

        if (primary != null) {
            // try bypass upstream and downstream
            Set<OperatorInput> downstreams = new HashSet<>();
            for (OperatorOutput output : operator.getOutputs()) {
                downstreams.addAll(output.getOpposites());
            }
            Operators.connectAll(primary.getOutput(), downstreams);
        }
        operator.disconnectAll();
        return true;
    }

    private boolean shrinkTrivialUpstreams(OperatorInput port) {
        Collection<OperatorOutput> upstreams = port.getOpposites();
        if (upstreams.isEmpty()) {
            return false;
        }
        List<MarkerOperator> trivials = new ArrayList<>(upstreams.size());
        boolean sawNonTrivial = false;
        for (OperatorOutput upstream : upstreams) {
            SubPlan.Input in = owner.findInput(upstream.getOwner());
            if (in != null && in.getOpposites().isEmpty()) {
                trivials.add(in.getOperator());
            } else {
                sawNonTrivial = true;
            }
        }
        if (trivials.isEmpty()) {
            return false;
        }
        boolean changed = false;
        if (sawNonTrivial) {
            for (MarkerOperator trivial : trivials) {
                assert port.isConnected(trivial.getOutput());
                port.disconnect(trivial.getOutput());
                changed = true;
            }
        } else {
            long minId = trivials.get(0).getSerialNumber();
            for (int i = 1, n = trivials.size(); i < n; i++) {
                minId = Math.min(minId, trivials.get(i).getSerialNumber());
            }
            for (MarkerOperator trivial : trivials) {
                // disconnect from operator expect which has minimum serial number (for stable operations)
                if (trivial.getSerialNumber() == minId) {
                    continue;
                }
                port.disconnect(trivial.getOutput());
                changed = true;
            }
        }
        return changed;
    }

    public boolean applyDuplicateCheckpointElimination() {
        if (operators.isEmpty()) {
            return false;
        }
        if (attributes.contains(Attribute.INPUT) == false
                || attributes.contains(Attribute.CHECKPOINT) == false) {
            return false;
        }
        boolean changed = false;
        for (Operator operator : operators) {
            changed |= applyDuplicateCheckpointElimination(operator);
        }
        return changed;
    }

    private boolean applyDuplicateCheckpointElimination(Operator operator) {
        if (Operators.hasSuccessors(operator) == false) {
            return false;
        }
        // c0 ===> $ +--------+ c1 ===> c1
        //            \- o0 -/
        // >>>
        // c0 +======================+> c1
        //     \=> $ --- o0 --- c1 =/
        boolean changed = false;
        BasicSubPlan.BasicInput input = owner.findInput(operator);
        assert input != null;
        Set<BasicSubPlan.BasicOutput> upstreams = input.getOpposites();
        OperatorOutput port = input.getOperator().getOutput();
        for (OperatorInput opposite : port.getOpposites()) {
            Operator succ = opposite.getOwner();
            BasicSubPlan.BasicOutput output = owner.findOutput(succ);
            if (output == null) {
                continue;
            }
            if (PlanMarkers.get(succ) != PlanMarker.CHECKPOINT) {
                continue;
            }
            Set<BasicSubPlan.BasicInput> downstreams = output.getOpposites();
            for (BasicSubPlan.BasicOutput upstream : upstreams) {
                for (BasicSubPlan.BasicInput downstream : downstreams) {
                    assert upstream.isConnected(downstream) == false;
                    upstream.connect(downstream);
                }
            }
            port.disconnect(opposite);
            changed = true;
        }
        return changed;
    }

    // has-same-inputs and is-isomorphic => has-equivalent-outputs
    private boolean hasSameInputs(Operator a, Operator b) {
        assert operators.contains(a);
        assert operators.contains(b);
        if (attributes.contains(Attribute.INPUT)) {
            SubPlan.Input aPort = owner.findInput(a);
            SubPlan.Input bPort = owner.findInput(b);
            assert aPort != null;
            assert bPort != null;
            return aPort.getOpposites().equals(bPort.getOpposites());
        } else {
            List<OperatorInput> aPorts = a.getInputs();
            List<OperatorInput> bPorts = b.getInputs();
            assert aPorts.size() == bPorts.size();
            for (int i = 0, n = aPorts.size(); i < n; i++) {
                OperatorInput aPort = aPorts.get(i);
                OperatorInput bPort = bPorts.get(i);
                if (aPort.hasSameOpposites(bPort) == false) {
                    return false;
                }
            }
            return true;
        }
    }

    private boolean hasSameBroadcastInputs(Operator a, Operator b) {
        assert operators.contains(a);
        assert operators.contains(b);
        BitSet indices = broadcastInputIndices;
        if (indices.isEmpty()) {
            return true;
        }
        // sub-plan inputs must not have any broadcast inputs
        assert attributes.contains(Attribute.INPUT) == false;
        List<OperatorInput> aPorts = a.getInputs();
        List<OperatorInput> bPorts = b.getInputs();
        assert aPorts.size() == bPorts.size();
        for (int i = indices.nextSetBit(0), n = aPorts.size(); i >= 0 && i < n; i = indices.nextSetBit(i + 1)) {
            OperatorInput aPort = aPorts.get(i);
            OperatorInput bPort = bPorts.get(i);
            if (aPort.hasSameOpposites(bPort) == false) {
                return false;
            }
        }
        return true;
    }

    private boolean hasSameOutputs(Operator a, Operator b) {
        assert operators.contains(a);
        assert operators.contains(b);
        if (attributes.contains(Attribute.OUTPUT)) {
            SubPlan.Output aPort = owner.findOutput(a);
            SubPlan.Output bPort = owner.findOutput(b);
            assert aPort != null;
            assert bPort != null;
            return aPort.getOpposites().equals(bPort.getOpposites());
        } else {
            List<OperatorOutput> aPorts = a.getOutputs();
            List<OperatorOutput> bPorts = b.getOutputs();
            assert aPorts.size() == bPorts.size();
            for (int i = 0, n = aPorts.size(); i < n; i++) {
                OperatorOutput aPort = aPorts.get(i);
                OperatorOutput bPort = bPorts.get(i);
                if (aPort.hasSameOpposites(bPort) == false) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public String toString() {
        if (operators.isEmpty()) {
            return "OperatorGroup(+0)"; //$NON-NLS-1$
        } else {
            Operator first = operators.iterator().next();
            return MessageFormat.format(
                    "OperatorGroup{2}({0}+{1})", //$NON-NLS-1$
                    first,
                    operators.size() - 1,
                    attributes);
        }
    }

    @FunctionalInterface
    private interface Applier {

        boolean apply(Operator base, Operator target);
    }

    public static final class GroupInfo {

        final Object id;

        final BitSet broadcastInputIndices;

        final Set<Attribute> attributes;

        GroupInfo(Object id, BitSet broadcastInputIndices, Set<Attribute> attributes) {
            this.id = id;
            this.broadcastInputIndices = broadcastInputIndices;
            this.attributes = attributes;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hashCode(id);
            result = prime * result + broadcastInputIndices.hashCode();
            result = prime * result + attributes.hashCode();
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
            GroupInfo other = (GroupInfo) obj;
            if (!Objects.equals(id, other.id)) {
                return false;
            }
            if (!broadcastInputIndices.equals(other.broadcastInputIndices)) {
                return false;
            }
            if (!attributes.equals(other.attributes)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return MessageFormat.format("Group({0})", id); //$NON-NLS-1$
        }
    }

    public enum Attribute {

        /**
         * Is extract kind.
         */
        EXTRACT_KIND,

        /**
         * Is sub-plan inputs.
         */
        INPUT,

        /**
         * Is sub-plan outputs.
         */
        OUTPUT,

        /**
         * Is generator.
         */
        GENERATOR,

        /**
         * Is consumer.
         */
        CONSUMER,

        /**
         * Is BEGIN plan markers.
         */
        BEGIN,

        /**
         * Is END plan markers.
         */
        END,

        /**
         * Is CHECKPOINT plan markers.
         */
        CHECKPOINT,

        /**
         * Is GATHER plan markers.
         */
        GATHER,

        /**
         * Is BROADCAST plan markers.
         */
        BROADCAST,
    }
}
