package com.asakusafw.lang.compiler.planning;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.planning.basic.BasicPlan;
import com.asakusafw.lang.compiler.planning.basic.BasicSubPlan;

/**
 * Organizes a {@link Plan}.
 */
public final class PlanBuilder {

    private final BasicPlan plan = new BasicPlan();

    private final Set<Operator> sourceOperators;

    // copy -> source
    private final Map<Operator, Operator> toSource = new HashMap<>();

    private PlanBuilder(Set<Operator> operators) {
        this.sourceOperators = operators;
    }

    /**
     * Creates a new instance with the source operators.
     * @param operators the source operators
     * @return the created instance
     */
    public static PlanBuilder from(Set<? extends Operator> operators) {
        return new PlanBuilder(Operators.getTransitiveConnected(operators));
    }

    /**
     * Adds a sub-plan with specified inputs and outputs.
     * <ol>
     * <li> inputs and outputs are neither empty </li>
     * <li> inputs and outputs are disjoint </li>
     * <li> each input must be a plan marker </li>
     * <li> each input must be in source operators </li>
     * <li> nearest forward reachable plan markers of each input must not contain any other inputs </li>
     * <li> nearest forward reachable plan markers of each input must contain at least one output </li>
     * <li> each output must be a plan marker </li>
     * <li> each output must be in source operators </li>
     * <li> nearest backward reachable plan markers of each output must not contain any other outputs </li>
     * <li> nearest backward reachable plan markers of each output must contain at least one input </li>
     * </ol>
     *
     * This organizes a sub-plan with following properties:
     * <ol>
     * <li> all operators (include inputs and outputs) are copied from the original sources </li>
     * <li> each operator connection is limited in the sub-plan </li>
     * <li> each input of sub-plan is copied from the original input operator </li>
     * <li> each output of sub-plan is copied from the original output operator </li>
     * <li> each operator is reachable from at least one input </li>
     * <li> each operator is reachable to at least one output </li>
     * <li>
     *      if some outputs are in the other sub-plan's inputs,
     *      the organizing sub-plan will become a predecessor of the target sub-plans
     * </li>
     * <li>
     *      if some inputs are in the other sub-plan's outputs,
     *      the organizing sub-plan will become a successor of the target sub-plans
     * </li>
     * </ol>
     *
     * @param inputs the input operators for the target sub-plan
     * @param outputs the output operators for the target sub-plan
     * @return this
     */
    public PlanBuilder add(Collection<? extends Operator> inputs, Collection<? extends Operator> outputs) {
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("inputs must not be empty");
        }
        if (outputs.isEmpty()) {
            throw new IllegalArgumentException("outputs must not be empty");
        }

        Set<MarkerOperator> in = asPlanMarkers(inputs);
        Set<MarkerOperator> out = asPlanMarkers(outputs);
        validateDisjoint(in, out);
        validateSource(in);
        validateSource(out);
        validateReachable(in, out);
        Set<Operator> members = computeRange(in, out);
        Map<Operator, Operator> sourceToCopy = OperatorGraph.copy(members);

        Set<MarkerOperator> copyInputs = applyMarkers(sourceToCopy, in);
        Set<MarkerOperator> copyOutputs = applyMarkers(sourceToCopy, out);
        plan.addElement(copyInputs, copyOutputs);

        // add copy->source info
        for (Map.Entry<Operator, Operator> entry : sourceToCopy.entrySet()) {
            Operator source = entry.getKey();
            Operator copy = entry.getValue();
            assert toSource.containsKey(copy) == false : copy;
            toSource.put(copy, source);
        }
        return this;
    }

    private void validateDisjoint(Set<MarkerOperator> in, Set<MarkerOperator> out) {
        Set<MarkerOperator> set = new HashSet<>(in);
        set.retainAll(out);
        if (set.isEmpty() == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "inputs and outputs must be disjoint: {0}",
                    set));
        }
    }

    private Set<MarkerOperator> applyMarkers(Map<Operator, Operator> sourceToCopy, Set<MarkerOperator> markers) {
        Set<MarkerOperator> results = new HashSet<>();
        for (MarkerOperator operator : markers) {
            assert sourceToCopy.containsKey(operator) : operator;
            Operator copy = sourceToCopy.get(operator);
            assert PlanMarkers.get(copy) != null : copy;
            results.add((MarkerOperator) copy);
        }
        return results;
    }

    private void validateSource(Collection<? extends Operator> operators) {
        for (Operator operator : operators) {
            if (sourceOperators.contains(operator) == false) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "operator must be in source operators: {0}",
                        operator));
            }
        }
    }

    private static Set<MarkerOperator> asPlanMarkers(Collection<? extends Operator> operators) {
        Set<MarkerOperator> results = new LinkedHashSet<>();
        for (Operator operator : operators) {
            if (PlanMarkers.get(operator) == null) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "operator must be a plan marker: {0}",
                        operator));
            }
            results.add((MarkerOperator) operator);
        }
        return results;
    }

    private void validateReachable(Set<MarkerOperator> inputs, Set<MarkerOperator> outputs) {
        Set<Operator> saw = new LinkedHashSet<Operator>();
        for (MarkerOperator operator : inputs) {
            Set<Operator> reachables = Operators.findNearestReachableSuccessors(
                    operator.getOutputs(), Planning.PLAN_MARKERS);
            for (MarkerOperator other : inputs) {
                if (reachables.contains(other)) {
                    throw new IllegalArgumentException(MessageFormat.format(
                            "input must not be reachable to other inputs: {0} -> {1}",
                            operator,
                            other));
                }
            }
            reachables.retainAll(outputs);
            if (reachables.isEmpty()) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "input must be reachable to at least one output: {0}",
                        operator));
            }
            saw.addAll(reachables);
        }
        if (saw.size() < outputs.size()) {
            for (MarkerOperator operator : outputs) {
                if (saw.contains(operator) == false) {
                    throw new IllegalArgumentException(MessageFormat.format(
                            "output must be reachable to at least one input: {0}",
                            operator));
                }
            }
        }
        for (MarkerOperator operator : outputs) {
            Set<Operator> reachables = Operators.findNearestReachablePredecessors(
                    operator.getInputs(), Planning.PLAN_MARKERS);
            for (MarkerOperator other : outputs) {
                if (reachables.contains(other)) {
                    throw new IllegalArgumentException(MessageFormat.format(
                            "output must be reachable to other outputs: {0} -> {1}",
                            operator,
                            other));
                }
            }
        }
    }

    private Set<Operator> computeRange(Set<MarkerOperator> in, Set<MarkerOperator> out) {
        Set<Operator> range = new HashSet<>();
        range.addAll(Operators.collectUntilNearestReachableSuccessors(
                Operators.getOutputs(in),
                Planning.PLAN_MARKERS,
                true));
        range.retainAll(Operators.collectUntilNearestReachablePredecessors(
                Operators.getInputs(out),
                Planning.PLAN_MARKERS,
                true));
        range.addAll(in);
        range.addAll(out);
        return range;
    }

    /**
     * Returns the built execution plan.
     * @return the detail of execution plan.
     */
    public PlanDetail build() {
        connectSubPlans();
        PlanDetail detail = new PlanDetail(plan, toSource);
        return detail;
    }

    private void connectSubPlans() {
        Map<MarkerOperator, Set<BasicSubPlan.BasicOutput>> sourceToOutputs = new HashMap<>();
        for (BasicSubPlan sub : plan.getElements()) {
            for (BasicSubPlan.BasicOutput output : sub.getOutputs()) {
                Operator source = toSource.get(output.getOperator());
                assert source != null;
                assert PlanMarkers.get(source) != null : source;
                Set<BasicSubPlan.BasicOutput> outputs = sourceToOutputs.get(source);
                if (outputs == null) {
                    outputs = new HashSet<>();
                    sourceToOutputs.put((MarkerOperator) source, outputs);
                }
                outputs.add(output);
            }
        }
        for (BasicSubPlan sub : plan.getElements()) {
            for (BasicSubPlan.BasicInput input : sub.getInputs()) {
                Operator source = toSource.get(input.getOperator());
                assert source != null;
                sourceToOutputs.get(source);
                Set<BasicSubPlan.BasicOutput> outputs = sourceToOutputs.get(source);
                if (outputs != null) {
                    for (BasicSubPlan.BasicOutput output : outputs) {
                        input.connect(output);
                    }
                }
            }
        }
    }
}
