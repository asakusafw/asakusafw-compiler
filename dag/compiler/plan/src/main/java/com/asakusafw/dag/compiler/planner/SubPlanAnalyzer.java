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
package com.asakusafw.dag.compiler.planner;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.dag.compiler.model.plan.InputSpec;
import com.asakusafw.dag.compiler.model.plan.InputSpec.InputOption;
import com.asakusafw.dag.compiler.model.plan.InputSpec.InputType;
import com.asakusafw.dag.compiler.model.plan.OutputSpec;
import com.asakusafw.dag.compiler.model.plan.OutputSpec.OutputOption;
import com.asakusafw.dag.compiler.model.plan.OutputSpec.OutputType;
import com.asakusafw.dag.compiler.model.plan.VertexSpec;
import com.asakusafw.dag.compiler.model.plan.VertexSpec.OperationOption;
import com.asakusafw.dag.compiler.model.plan.VertexSpec.OperationType;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizers;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputAttribute;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * Provides helpful information for consequent code generation phase about {@link SubPlan}.
 * @since 0.4.0
 */
public final class SubPlanAnalyzer {

    private static final TypeDescription VOID_TYPE = Descriptions.typeOf(void.class);

    private final PlanDetail detail;

    private final Map<Operator, OperatorClass> operatorClasses;

    private final GroupKeyUnifier groupKeys = new GroupKeyUnifier();

    private final Map<SubPlan, String> vertexIds;

    private final Map<SubPlan.Input, String> inputIds;

    private final Map<SubPlan.Output, String> outputIds;

    private final Map<SubPlan, VertexSpec> vertexSpecs = new HashMap<>();

    private final Map<SubPlan.Input, InputSpec> inputSpecs = new HashMap<>();

    private final Map<SubPlan.Output, OutputSpec> outputSpecs = new HashMap<>();

    private SubPlanAnalyzer(
            PlanDetail detail,
            Map<Operator, OperatorClass> operatorClasses,
            Map<SubPlan, String> vertexIds,
            Map<SubPlan.Input, String> inputIds,
            Map<SubPlan.Output, String> outputIds) {
        this.detail = detail;
        this.operatorClasses = operatorClasses;
        this.vertexIds = vertexIds;
        this.inputIds = inputIds;
        this.outputIds = outputIds;
    }

    /**
     * Creates a new instance.
     * @param context the current context
     * @param detail the detail of the target plan
     * @param normalized the normalized operator graph, which is origin of the target plan
     * @return the created instance
     */
    public static SubPlanAnalyzer newInstance(
            PlanningContext context,
            PlanDetail detail,
            OperatorGraph normalized) {
        Map<Operator, OperatorClass> characteristics = OperatorCharacterizers.apply(
                context.getOptimizerContext(),
                context.getEstimator(),
                context.getClassifier(),
                normalized.getOperators(false));
        Plan plan = detail.getPlan();
        Map<SubPlan, String> vIds = Util.computeIds("v", Util.sortElements(plan));
        Map<SubPlan.Input, String> iIds = Util.computeIds("i", plan.getElements(), Util::sortInputs);
        Map<SubPlan.Output, String> oIds = Util.computeIds("o", plan.getElements(), Util::sortOutputs);
        return new SubPlanAnalyzer(detail, characteristics, vIds, iIds, oIds);
    }

    /**
     * Analyzes a sub-plan.
     * @param sub the target sub-plan
     * @return the analyzed information
     */
    public VertexSpec analyze(SubPlan sub) {
        VertexSpec cached = vertexSpecs.get(sub);
        if (cached != null) {
            return cached;
        }
        VertexSpec info = analyze0(sub);
        vertexSpecs.put(info.getOrigin(), info);
        return info;
    }

    private VertexSpec analyze0(SubPlan sub) {
        Operator primaryOperator = getPrimaryOperator(sub);
        OperationType driverType;
        Set<OperationOption> driverOptions;
        if (primaryOperator == null) {
            driverType = OperationType.EXTRACT;
            driverOptions = Collections.emptySet();
        } else {
            OperatorClass operatorClass = getOperatorClass(primaryOperator);
            driverType = computeOperationType(operatorClass);
            driverOptions = computeOperationOptions(operatorClass);
        }
        return new VertexSpec(
                sub, Invariants.requireNonNull(vertexIds.get(sub)),
                driverType, driverOptions, primaryOperator);
    }

    private Operator getPrimaryOperator(SubPlan sub) {
        Set<Operator> candidates = new LinkedHashSet<>();
        for (SubPlan.Input port : sub.getInputs()) {
            if (Util.isPrimaryInput(port)) {
                for (Operator candidate : Operators.getSuccessors(port.getOperator())) {
                    // sub-plan's output must not be a primary operator
                    if (sub.findOutput(candidate) == null) {
                        candidates.add(candidate);
                    }
                }
            }
        }
        // primary operator must be unique
        if (candidates.isEmpty()) {
            return null;
        }
        // if we cannot determines the unique primary operator,
        // primary input type of each candidate must be 'RECORD'
        if (candidates.size() >= 2) {
            for (Operator operator : candidates) {
                OperatorClass operatorClass = getOperatorClass(operator);
                Invariants.require(operatorClass.getPrimaryInputType() == OperatorClass.InputType.RECORD);
            }
            return null;
        }
        return candidates.iterator().next();
    }

    private OperatorClass getOperatorClass(Operator operator) {
        Operator source = detail.getSource(operator);
        assert source != null;
        OperatorClass operatorClass = operatorClasses.get(source);
        assert operatorClass != null;
        return operatorClass;
    }

    private static OperationType computeOperationType(OperatorClass operatorClass) {
        Operator operator = operatorClass.getOperator();
        if (operator.getOperatorKind() == OperatorKind.INPUT) {
            return OperationType.EXTRACT;
        } else if (operator.getOperatorKind() == OperatorKind.OUTPUT) {
            return OperationType.OUTPUT;
        }
        if (operatorClass.getPrimaryInputType() == OperatorClass.InputType.RECORD) {
            return OperationType.EXTRACT;
        } else if (operatorClass.getPrimaryInputType() == OperatorClass.InputType.GROUP) {
            return OperationType.CO_GROUP;
        } else {
            throw new IllegalStateException();
        }
    }

    private static Set<OperationOption> computeOperationOptions(OperatorClass operatorClass) {
        if (isPreAggregation(operatorClass)) {
            return Collections.singleton(OperationOption.PRE_AGGREGATION);
        }
        OperatorKind kind = operatorClass.getOperator().getOperatorKind();
        switch (kind) {
        case INPUT:
            return Collections.singleton(OperationOption.EXTERNAL_INPUT);
        case OUTPUT:
            return Collections.singleton(OperationOption.EXTERNAL_OUTPUT);
        default:
            return Collections.emptySet();
        }
    }

    /**
     * Analyzes input of sub-plan.
     * @param input the target input
     * @return the analyzed information
     */
    public InputSpec analyze(SubPlan.Input input) {
        InputSpec cached = inputSpecs.get(input);
        if (cached != null) {
            return cached;
        }
        InputSpec info = analyze0(input);
        inputSpecs.put(info.getOrigin(), info);
        return info;
    }

    private InputSpec analyze0(SubPlan.Input input) {
        String id = Invariants.requireNonNull(inputIds.get(input));
        InputType type = computeInputType(input);
        Set<InputOption> options = computeInputOptions(input, type);
        TypeDescription dataType = computeInputDataType(input, type);
        switch (type) {
        case NO_DATA:
        case EXTRACT:
            return new InputSpec(input, id, dataType, type, options, null);
        case BROADCAST:
        case CO_GROUP:
            return new InputSpec(input, id, dataType, type, options, computeInputGroup(input));
        default:
            throw new AssertionError(type);
        }
    }

    private InputType computeInputType(SubPlan.Input input) {
        if (input.getOpposites().isEmpty()) {
            return InputType.NO_DATA;
        }
        if (Util.isPrimaryInput(input) == false) {
            return InputType.BROADCAST;
        }
        VertexSpec info = analyze(input.getOwner());
        switch (info.getOperationType()) {
        case EXTRACT:
        case OUTPUT:
            return InputType.EXTRACT;
        case CO_GROUP:
            return InputType.CO_GROUP;
        default:
            throw new AssertionError(info);
        }
    }

    private Set<InputOption> computeInputOptions(SubPlan.Input input, InputType type) {
        EnumSet<InputOption> results = EnumSet.noneOf(InputOption.class);
        if (Util.isPrimaryInput(input)) {
            results.add(InputOption.PRIMARY);
        }
        if (isSpillOut(input)) {
            results.add(InputOption.SPILL_OUT);
        }
        if (isReadOnce(input)) {
            results.add(InputOption.READ_ONCE);
        }
        return results;
    }

    private boolean isSpillOut(SubPlan.Input input) {
        for (OperatorInput consumer : input.getOperator().getOutput().getOpposites()) {
            OperatorClass info = getOperatorClass(consumer.getOwner());
            OperatorInput resolved = info.getOperator().findInput(consumer.getName());
            if (info.getAttributes(resolved).contains(InputAttribute.ESCAPED)) {
                return true;
            }
        }
        return false;
    }

    private boolean isReadOnce(SubPlan.Input input) {
        for (OperatorInput consumer : input.getOperator().getOutput().getOpposites()) {
            OperatorClass info = getOperatorClass(consumer.getOwner());
            OperatorInput resolved = info.getOperator().findInput(consumer.getName());
            if (info.getAttributes(resolved).contains(InputAttribute.VOALTILE) == false) {
                return false;
            }
        }
        return true;
    }

    private TypeDescription computeInputDataType(SubPlan.Input input, InputType type) {
        if (type == InputType.NO_DATA) {
            return VOID_TYPE;
        } else if (type == InputType.CO_GROUP && isAggregate(input)) {
            VertexSpec info = analyze(input.getOwner());
            Operator primary = info.getPrimaryOperator();
            Invariants.requireNonNull(primary);
            Invariants.require(primary.getOutputs().size() == 1);
            return primary.getOutputs().get(0).getDataType();
        }
        return input.getOperator().getDataType();
    }

    private Group computeInputGroup(SubPlan.Input input) {
        Group result = Invariants.requireNonNull(groupKeys.get(input));
        return result;
    }

    /**
     * Analyzes output of sub-plan.
     * @param output the target output
     * @return the analyzed information
     */
    public OutputSpec analyze(SubPlan.Output output) {
        OutputSpec cached = outputSpecs.get(output);
        if (cached != null) {
            return cached;
        }
        OutputSpec info = analyze0(output);
        outputSpecs.put(info.getOrigin(), info);
        return info;
    }

    private OutputSpec analyze0(SubPlan.Output output) {
        String id = Invariants.requireNonNull(outputIds.get(output));
        OutputType type = computeOutputType(output);
        TypeDescription sourceType = output.getOperator().getDataType();
        TypeDescription dataType = computeOutputDataType(output, type);
        switch (type) {
        case DISCARD:
        case VALUE:
            return new OutputSpec(output, id, type, sourceType, dataType, Collections.emptySet(),
                    null, null);
        case KEY_VALUE:
            if (isAggregate(output)) {
                Set<OutputOption> options = EnumSet.noneOf(OutputOption.class);
                Operator aggregator = computeOutputAggregator(output);
                OperatorClass operatorClass = getOperatorClass(aggregator);
                if (isPreAggregation(operatorClass)) {
                    options.add(OutputOption.PRE_AGGREGATION);
                }
                return new OutputSpec(output, id, type, sourceType, dataType, options,
                        computeOutputGroup(output), aggregator);
            } else {
                return new OutputSpec(output, id, type, sourceType, dataType, Collections.emptySet(),
                        computeOutputGroup(output), null);
            }
        case BROADCAST:
            return new OutputSpec(output, id, type, sourceType, dataType, Collections.emptySet(),
                    computeOutputGroup(output), null);
        default:
            throw new AssertionError(type);
        }
    }

    private OutputType computeOutputType(SubPlan.Output output) {
        Set<? extends SubPlan.Input> downstreams = output.getOpposites();
        if (downstreams.isEmpty()) {
            return OutputType.DISCARD;
        }
        OutputType result = null;
        for (SubPlan.Input downstream : downstreams) {
            OutputType candidate = computeRequiredOutputType(downstream);
            if (result == null) {
                result = candidate;
            } else {
                Invariants.require(candidate == result);
            }
        }
        assert result != null;
        return result;
    }

    private TypeDescription computeOutputDataType(SubPlan.Output output, OutputType type) {
        if (type == OutputType.DISCARD) {
            return VOID_TYPE;
        } else if (type == OutputType.KEY_VALUE && isAggregate(output)) {
            Operator aggregator = computeOutputAggregator(output);
            Invariants.requireNonNull(aggregator);
            Invariants.require(aggregator.getOutputs().size() == 1);
            return aggregator.getOutputs().get(0).getDataType();
        }
        return output.getOperator().getDataType();
    }

    private OutputType computeRequiredOutputType(SubPlan.Input downstream) {
        if (Util.isPrimaryInput(downstream) == false) {
            return OutputType.BROADCAST;
        }
        VertexSpec info = analyze(downstream.getOwner());
        switch (info.getOperationType()) {
        case EXTRACT:
        case OUTPUT:
            return OutputType.VALUE;
        case CO_GROUP:
            return OutputType.KEY_VALUE;
        default:
            throw new AssertionError(info);
        }
    }

    private Group computeOutputGroup(SubPlan.Output output) {
        return Invariants.requireNonNull(groupKeys.get(output));
    }

    private static Operator computeOutputAggregator(SubPlan.Output output) {
        OperatorInput result = null;
        for (SubPlan.Input opposite : output.getOpposites()) {
            for (OperatorInput candidate : opposite.getOperator().getOutput().getOpposites()) {
                if (result == null) {
                    result = candidate;
                } else {
                    Invariants.require(Util.isCompatible(result, candidate));
                }
            }
        }
        assert result != null;
        return result.getOwner();
    }

    private boolean isAggregate(SubPlan.Input input) {
        VertexSpec info = analyze(input.getOwner());
        Operator primary = info.getPrimaryOperator();
        Invariants.requireNonNull(primary);
        OperatorClass operatorClass = getOperatorClass(primary);
        return isAggregate(operatorClass);
    }

    private boolean isAggregate(SubPlan.Output output) {
        for (SubPlan.Input opposite : output.getOpposites()) {
            if (isAggregate(opposite)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isAggregate(OperatorClass operatorClass) {
        Set<OperatorInput> primary = operatorClass.getPrimaryInputs();
        if (primary.size() == 1) {
            Set<InputAttribute> attributes = operatorClass.getAttributes(primary.iterator().next());
            return attributes.contains(InputAttribute.AGGREATE);
        }
        return false;
    }

    private static boolean isPreAggregation(OperatorClass operatorClass) {
        if (isAggregate(operatorClass) == false) {
            return false;
        }
        Set<OperatorInput> inputs = operatorClass.getInputs();

        // NOTE: if aggregate accepts some data-tables, the partial reduction will be disabled
        if (inputs.size() == 1) {
            Set<InputAttribute> attrs = operatorClass.getAttributes(
                    operatorClass.getPrimaryInputs().iterator().next());
            return attrs.contains(InputAttribute.PARTIAL_REDUCTION);
        }
        return false;
    }
}
