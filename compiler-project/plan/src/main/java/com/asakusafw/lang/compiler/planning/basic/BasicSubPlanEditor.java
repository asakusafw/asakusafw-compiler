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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.planning.OperatorEquivalence;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.compiler.planning.basic.OperatorGroup.Attribute;
import com.asakusafw.lang.compiler.planning.basic.OperatorGroup.GroupInfo;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * An editor for {@link BasicSubPlan}.
 */
final class BasicSubPlanEditor {

    private final BasicSubPlan target;

    private final Map<Operator, OperatorGroup> groupMap;

    private final List<OperatorGroup> forward = new LinkedList<>();

    private final List<OperatorGroup> backward = new LinkedList<>();

    /**
     * Creates a new instance.
     * @param target the target sub-plan
     * @param equivalence tester for operator isomorphism
     */
    public BasicSubPlanEditor(BasicSubPlan target, OperatorEquivalence equivalence) {
        this.target = target;
        this.groupMap = computeGroupMap(target, equivalence);
        Graph<OperatorGroup> graph = computeOperatorGroups(target, groupMap);
        forward.addAll(Graphs.sortPostOrder(graph));
        backward.addAll(Graphs.sortPostOrder(Graphs.transpose(graph)));
    }

    private static Map<Operator, OperatorGroup> computeGroupMap(BasicSubPlan sub, OperatorEquivalence equivalence) {
        Map<OperatorGroup.GroupInfo, Set<Operator>> partitions = computeGroupPartitions(sub, equivalence);
        Map<Operator, OperatorGroup> results = new HashMap<>();
        for (Map.Entry<OperatorGroup.GroupInfo, Set<Operator>> entry : partitions.entrySet()) {
            Set<Operator> operators = entry.getValue();
            OperatorGroup.GroupInfo info = entry.getKey();
            OperatorGroup group = new OperatorGroup(sub, operators, info.broadcastInputIndices, info.attributes);
            for (Operator operator : operators) {
                results.put(operator, group);
            }
        }
        return results;
    }

    private static Map<GroupInfo, Set<Operator>> computeGroupPartitions(SubPlan sub, OperatorEquivalence equivalence) {
        Set<Operator> inputs = toOperators(sub.getInputs());
        Set<Operator> outputs = toOperators(sub.getOutputs());
        Set<OperatorInput> broadcastInputs = Util.computeBroadcastInputs(inputs, outputs);
        Map<OperatorGroup.GroupInfo, Set<Operator>> results = new HashMap<>();
        for (Operator operator : sub.getOperators()) {
            Object id = equivalence.extract(sub, operator);
            BitSet broadcastIndices = getBroadcastInputIndices(operator, broadcastInputs);
            Set<OperatorGroup.Attribute> attributes = OperatorGroup.getAttributes(sub, operator);
            OperatorGroup.GroupInfo info = new OperatorGroup.GroupInfo(id, broadcastIndices, attributes);
            add(results, info, operator);
        }
        return results;
    }

    private static <K, V> void add(Map<K, Set<V>> map, K key, V value) {
        Set<V> values = map.get(key);
        if (values == null) {
            values = new HashSet<>();
            map.put(key, values);
        }
        assert values.contains(value) == false;
        values.add(value);
    }

    private static BitSet getBroadcastInputIndices(Operator operator, Set<OperatorInput> broadcastInputs) {
        List<OperatorInput> inputs = operator.getInputs();
        BitSet results = new BitSet(inputs.size());
        for (int i = 0, n = inputs.size(); i < n; i++) {
            if (broadcastInputs.contains(inputs.get(i))) {
                results.set(i);
            }
        }
        return results;
    }

    private static Graph<OperatorGroup> computeOperatorGroups(SubPlan sub, Map<Operator, OperatorGroup> map) {
        Graph<OperatorGroup> results = Graphs.newInstance();
        for (Operator operator : sub.getOperators()) {
            OperatorGroup group = map.get(operator);
            results.addNode(group);
            assert group != null;
            Set<Operator> preds = Operators.getPredecessors(operator);
            for (Operator pred : preds) {
                OperatorGroup blocker = map.get(pred);
                assert blocker != null;
                results.addEdge(group, blocker);
            }
        }
        return results;
    }

    /**
     * Returns the editing target.
     * @return the target
     */
    public BasicSubPlan getTarget() {
        return target;
    }

    /**
     * Returns operator groups sorted from upstream to downstream.
     * @return operator groups
     */
    public List<OperatorGroup> getOperatorGroupsForward() {
        return forward;
    }

    /**
     * Returns operator groups sorted from downstream to upstream.
     * @return operator groups
     */
    public List<OperatorGroup> getOperatorGroupsBackward() {
        return backward;
    }

    /**
     * Re-validates the plan constraints and removes redundant elements.
     * @return {@code true} if any element was changed, otherwise {@code false}
     */
    public boolean revalidate() {
        boolean changed = false;
        changed |= removeRedundantInputs();
        changed |= removeRedundantOutputs();
        changed |= removeRedundantOperators();
        changed |= removeEmptyGroups();
        return changed;
    }

    private boolean removeRedundantInputs() {
        final Set<Operator> candidates = collectOpenPorts(target.getInputs());
        if (candidates.isEmpty()) {
            return false;
        }
        List<OperatorInput> generators = new ArrayList<>();
        for (Map.Entry<Operator, OperatorGroup> entry : groupMap.entrySet()) {
            Operator operator = entry.getKey();
            if (entry.getValue().has(Attribute.GENERATOR)) {
                generators.addAll(operator.getInputs());
            }
        }
        if (generators.isEmpty() == false) {
            Set<Operator> reachables =
                    Operators.findNearestReachablePredecessors(generators, Util.isMember(candidates));
            candidates.removeAll(reachables);
        }
        return removeAll(candidates);
    }

    private boolean removeRedundantOutputs() {
        final Set<Operator> candidates = collectOpenPorts(target.getOutputs());
        if (candidates.isEmpty()) {
            return false;
        }
        List<OperatorOutput> consumers = new ArrayList<>();
        for (Map.Entry<Operator, OperatorGroup> entry : groupMap.entrySet()) {
            Operator operator = entry.getKey();
            if (entry.getValue().has(Attribute.CONSUMER)) {
                consumers.addAll(operator.getOutputs());
            }
        }
        if (consumers.isEmpty() == false) {
            Set<Operator> reachables = Operators.findNearestReachableSuccessors(consumers, Util.isMember(candidates));
            candidates.removeAll(reachables);
        }
        return removeAll(candidates);
    }

    private static Set<Operator> collectOpenPorts(Collection<? extends SubPlan.Port> ports) {
        final Set<Operator> results = new HashSet<>();
        for (SubPlan.Port port : ports) {
            if (port.getOpposites().isEmpty()) {
                results.add(port.getOperator());
            }
        }
        return results;
    }

    private boolean removeAll(Collection<? extends Operator> operators) {
        boolean changed = false;
        for (Operator operator : operators) {
            OperatorGroup group = groupMap.remove(operator);
            if (group != null) {
                group.remove(operator);
                changed = true;
            }
        }
        return changed;
    }

    private boolean removeRedundantOperators() {
        boolean changed = false;
        Set<Operator> effectives = getEffectiveOperators();
        for (Iterator<Map.Entry<Operator, OperatorGroup>> iter = groupMap.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Operator, OperatorGroup> entry = iter.next();
            Operator operator = entry.getKey();
            if (effectives.remove(operator) == false) {
                iter.remove();
                OperatorGroup group = entry.getValue();
                group.remove(operator);
                changed = true;
            }
        }
        assert effectives.isEmpty() : effectives;
        return changed;
    }

    private Set<Operator> getEffectiveOperators() {
        Set<Operator> inputs = new HashSet<>(toOperators(target.getInputs()));
        Set<Operator> outputs = new HashSet<>(toOperators(target.getOutputs()));
        Set<Operator> inputReachables = new HashSet<>(inputs);
        inputReachables.addAll(Operators.getTransitiveSuccessors(Operators.getOutputs(inputs)));
        Set<Operator> outputReachables = new HashSet<>(outputs);
        outputReachables.addAll(Operators.getTransitivePredecessors(Operators.getInputs(outputs)));
        inputReachables.retainAll(outputReachables);
        return inputReachables;
    }

    private boolean removeEmptyGroups() {
        if (removeEmptyGroups(forward)) {
            removeEmptyGroups(backward);
            assert forward.size() == backward.size();
            return true;
        }
        return false;
    }

    private boolean removeEmptyGroups(List<OperatorGroup> groups) {
        boolean changed = false;
        for (Iterator<OperatorGroup> iter = groups.iterator(); iter.hasNext();) {
            OperatorGroup group = iter.next();
            if (group.isEmpty()) {
                changed = true;
                iter.remove();
            }
        }
        return changed;
    }

    /**
     * Returns whether the target sub-plan is empty or not.
     * @return {@code true} if the target sub-plan is empty, otherwise {@code false}
     */
    public boolean isEmpty() {
        assert forward.size() == backward.size();
        return forward.isEmpty();
    }

    private static Set<Operator> toOperators(Set<? extends SubPlan.Port> ports) {
        Set<Operator> results = new HashSet<>();
        for (SubPlan.Port port : ports) {
            results.add(port.getOperator());
        }
        return results;
    }
}
