package com.asakusafw.lang.compiler.planning;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.planning.basic.BasicPlan;
import com.asakusafw.lang.compiler.planning.basic.BasicPlanOptimizer;
import com.asakusafw.lang.compiler.planning.basic.SubPlanMerger;

/**
 * Assembles sub-plans and re-organize a new plan.
 */
public final class PlanAssembler {

    private final SubPlanMerger merger;

    private final Set<BasicPlanOptimizer.Option> options = EnumSet.noneOf(BasicPlanOptimizer.Option.class);

    /**
     * Creates a new instance with a plan as the re-organize target.
     * @param target detail of the target plan
     */
    public PlanAssembler(PlanDetail target) {
        this.merger = new SubPlanMerger(target);
    }

    /**
     * Enables <em>trivial output elimination</em> optimization.
     * @param enable {@code true} to enable the optimization, or {@code false} to disable it
     * @return this
     * @see com.asakusafw.lang.compiler.planning.basic.BasicPlanOptimizer.Option#TRIVIAL_OUTPUT_ELIMINATION
     */
    public PlanAssembler withTrivialOutputElimination(boolean enable) {
        setOption(BasicPlanOptimizer.Option.TRIVIAL_OUTPUT_ELIMINATION, enable);
        return this;
    }

    /**
     * Enables <em>redundant output elimination</em> optimization.
     * @param enable {@code true} to enable the optimization, or {@code false} to disable it
     * @return this
     * @see com.asakusafw.lang.compiler.planning.basic.BasicPlanOptimizer.Option#REDUNDANT_OUTPUT_ELIMINATION
     */
    public PlanAssembler withRedundantOutputElimination(boolean enable) {
        setOption(BasicPlanOptimizer.Option.REDUNDANT_OUTPUT_ELIMINATION, enable);
        return this;
    }

    /**
     * Enables <em>duplicate checkpoint elimination</em> optimization.
     * @param enable {@code true} to enable the optimization, or {@code false} to disable it
     * @return this
     * @see com.asakusafw.lang.compiler.planning.basic.BasicPlanOptimizer.Option#DUPLICATE_CHECKPOINT_ELIMINATION
     */
    public PlanAssembler withDuplicateCheckpointElimination(boolean enable) {
        setOption(BasicPlanOptimizer.Option.DUPLICATE_CHECKPOINT_ELIMINATION, enable);
        return this;
    }

    /**
     * Enables <em>union push down</em> optimization.
     * @param enable {@code true} to enable the optimization, or {@code false} to disable it
     * @return this
     * @see com.asakusafw.lang.compiler.planning.basic.BasicPlanOptimizer.Option#UNION_PUSH_DOWN
     */
    public PlanAssembler withUnionPushDown(boolean enable) {
        setOption(BasicPlanOptimizer.Option.UNION_PUSH_DOWN, enable);
        return this;
    }

    private void setOption(BasicPlanOptimizer.Option option, boolean enable) {
        if (enable) {
            options.add(option);
        } else {
            options.remove(option);
        }
    }

    /**
     * Adds a new sub-plan consists of the specified source sub-plans.
     * Each source sub-plan must be a member of the re-organize target plan.
     * @param sources the source sub-plan
     * @return this
     */
    public PlanAssembler add(Collection<? extends SubPlan> sources) {
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("sources must not be empty");
        }
        this.merger.add(sources);
        return this;
    }

    /**
     * Builds and returns a new plan using the previous assembly information.
     * @return detail of the assembled plan
     */
    public PlanDetail build() {
        PlanDetail merged = merger.build();
        Plan plan = merged.getPlan();
        assert plan instanceof BasicPlan;
        BasicPlanOptimizer optimizer = new BasicPlanOptimizer(options);
        optimizer.optimize((BasicPlan) plan);
        return restoreDetail(merged);
    }

    private PlanDetail restoreDetail(PlanDetail detail) {
        Map<Operator, Operator> mapping = new HashMap<>();
        for (SubPlan sub : detail.getPlan().getElements()) {
            for (Operator copy : sub.getOperators()) {
                Operator source = detail.getSource(copy);
                if (source != null) {
                    mapping.put(copy, source);
                }
            }
        }
        return new PlanDetail(detail.getPlan(), mapping);
    }
}
