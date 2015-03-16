package com.asakusafw.lang.compiler.planning.basic;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.planning.PlanBuilder;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Merges sub-plans.
 */
public class SubPlanMerger {

    private final PlanDetail sourceDetail;

    private final List<Set<SubPlan>> assemblies = new ArrayList<>();

    /**
     * Creates a new instance.
     * @param sourceDetail the merge target plan
     */
    public SubPlanMerger(PlanDetail sourceDetail) {
        this.sourceDetail = sourceDetail;
    }

    /**
     * Adds a new sub-plan consists of the specified source sub-plans.
     * Each source sub-plan must be a member of the re-organize target plan.
     * @param sources the source sub-plan
     * @return this
     */
    public SubPlanMerger add(Collection<? extends SubPlan> sources) {
        if (sources.isEmpty()) {
            return this;
        }
        for (SubPlan sub : sources) {
            if (sourceDetail.getPlan() != sub.getOwner()) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "sub-plan \"{0}\" must have the owner \"{2}\": {1}",
                        sub,
                        sub.getOwner(),
                        sourceDetail.getPlan()));
            }
        }
        this.assemblies.add(new LinkedHashSet<>(sources));
        return this;
    }

    /**
     * Adds a new sub-plan consists of the specified source sub-plans.
     * Each source sub-plan must be a member of the re-organize target plan.
     * @param sources the source sub-plan
     * @return this
     */
    public SubPlanMerger add(SubPlan... sources) {
        return add(Arrays.asList(sources));
    }

    /**
     * Builds a new plan which consists of the merged sub-plans.
     * @return the built plan
     */
    public PlanDetail build() {
        PlanBuilder builder = PlanBuilder.from(sourceDetail.getCopies());
        for (Set<SubPlan> assembly : assemblies) {
            List<MarkerOperator> inputs = new ArrayList<>();
            List<MarkerOperator> outputs = new ArrayList<>();
            for (SubPlan sub : assembly) {
                for (SubPlan.Input port : sub.getInputs()) {
                    inputs.add(port.getOperator());
                }
                for (SubPlan.Output port : sub.getOutputs()) {
                    outputs.add(port.getOperator());
                }
            }
            builder.add(inputs, outputs);
        }
        PlanDetail base = builder.build();
        restoreConnections(base);
        PlanDetail result = retargetSources(base);
        return result;
    }

    private void restoreConnections(PlanDetail assembled) {
        assert assembled.getPlan() instanceof BasicPlan;
        BasicPlan plan = (BasicPlan) assembled.getPlan();
        Map<SubPlan.Input, Set<BasicSubPlan.BasicInput>> inputMap = new HashMap<>();
        Map<SubPlan.Output, Set<BasicSubPlan.BasicOutput>> outputMap = new HashMap<>();
        for (BasicSubPlan sub : plan.getElements()) {
            for (BasicSubPlan.BasicInput port : sub.getInputs()) {
                Operator source = assembled.getSource(port.getOperator());
                assert source != null;
                SubPlan owner = sourceDetail.getOwner(source);
                assert owner != null;
                SubPlan.Input origin = owner.findInput(source);
                assert origin != null;
                Set<BasicSubPlan.BasicInput> copies = inputMap.get(origin);
                if (copies == null) {
                    copies = new HashSet<>();
                    inputMap.put(origin, copies);
                }
                copies.add(port);
            }
            for (BasicSubPlan.BasicOutput port : sub.getOutputs()) {
                Operator source = assembled.getSource(port.getOperator());
                assert source != null;
                SubPlan owner = sourceDetail.getOwner(source);
                assert owner != null;
                SubPlan.Output origin = owner.findOutput(source);
                assert origin != null;
                Set<BasicSubPlan.BasicOutput> copies = outputMap.get(origin);
                if (copies == null) {
                    copies = new HashSet<>();
                    outputMap.put(origin, copies);
                }
                copies.add(port);
            }
        }
        for (Map.Entry<SubPlan.Input, Set<BasicSubPlan.BasicInput>> entry : inputMap.entrySet()) {
            SubPlan.Input input = entry.getKey();
            Set<BasicSubPlan.BasicInput> inputCopies = entry.getValue();
            for (SubPlan.Output output : input.getOpposites()) {
                Set<BasicSubPlan.BasicOutput> outputCopies = outputMap.get(output);
                if (outputCopies == null) {
                    continue;
                }
                connect(outputCopies, inputCopies);
            }
        }
    }

    private void connect(Set<BasicSubPlan.BasicOutput> upstreams, Set<BasicSubPlan.BasicInput> downstreams) {
        for (BasicSubPlan.BasicOutput upstream : upstreams) {
            for (BasicSubPlan.BasicInput downstream : downstreams) {
                assert upstream.getOwner().getOwner() == downstream.getOwner().getOwner();
                upstream.connect(downstream);
            }
        }
    }

    private PlanDetail retargetSources(PlanDetail detail) {
        Map<Operator, Operator> copyToSource = new HashMap<>();
        for (Operator copy : detail.getCopies()) {
            Operator source = detail.getSource(copy);
            if (source != null) {
                Operator origin = sourceDetail.getSource(source);
                if (origin != null) {
                    copyToSource.put(copy, origin);
                }
            }
        }
        return new PlanDetail(detail.getPlan(), copyToSource);
    }
}
