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
package com.asakusafw.dag.compiler.planner;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Set;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.dag.compiler.model.plan.InputSpec.InputOption;
import com.asakusafw.dag.compiler.model.plan.InputSpec.InputType;
import com.asakusafw.dag.compiler.model.plan.OutputSpec.OutputOption;
import com.asakusafw.dag.compiler.model.plan.OutputSpec.OutputType;
import com.asakusafw.dag.compiler.model.plan.VertexSpec.OperationOption;
import com.asakusafw.dag.compiler.model.plan.VertexSpec.OperationType;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo.DataSize;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.vocabulary.attribute.ViewInfo;
import com.asakusafw.vocabulary.flow.processor.InputBuffer;
import com.asakusafw.vocabulary.flow.processor.PartialAggregation;
import com.asakusafw.vocabulary.operator.CoGroup;
import com.asakusafw.vocabulary.operator.Extract;
import com.asakusafw.vocabulary.operator.Fold;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;

/**
 * Test for {@link DagPlanning}.
 */
public class DagPlanningTest extends PlanningTestRoot {

    /**
     * simple case.
<pre>{@code
in --- out
==>
in --- *C --- out
}</pre>
     */
    @Test
    public void simple() {
        PlanDetail detail = DagPlanning.plan(context(), new MockOperators()
            .input("in", DataSize.LARGE)
            .output("out").connect("in", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(2));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("out"));

        assertThat(info(s0).toString(), s0, primaryOperator(isOperator("in")));
        assertThat(s0, operationType(is(OperationType.EXTRACT)));
        assertThat(s0, not(operationOption(is(OperationOption.PRE_AGGREGATION))));
        assertThat(input(s0), inputType(is(InputType.NO_DATA)));
        assertThat(input(s0), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s0), inputGroup(is(nullValue())));
        assertThat(output(s0), outputType(is(OutputType.VALUE)));
        assertThat(output(s0), outputGroup(is(nullValue())));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(info(s1).toString(), s1, primaryOperator(isOperator("out")));
        assertThat(s1, operationType(is(OperationType.OUTPUT)));
        assertThat(s1, not(operationOption(is(OperationOption.PRE_AGGREGATION))));
        assertThat(input(s1), inputType(is(InputType.EXTRACT)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), inputGroup(is(nullValue())));
        assertThat(output(s1), outputType(is(OutputType.DISCARD)));
        assertThat(output(s1), outputGroup(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * extract kind.
<pre>{@code
in --- c0 --- o0 --- out
==>
in --- *C --- o0 --- *C --- out
}</pre>
     */
    @Test
    public void extract_kind() {
        PlanDetail detail = DagPlanning.plan(context(), new MockOperators()
            .input("in", DataSize.LARGE)
            .operator(cp(), "c0").connect("in", "c0")
            .operator(op(Extract.class, "extract"), "o0").connect("c0", "o0")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        assertThat("checkpoint operator should be replaced", mock.all(), not(hasItem(isOperator("c0"))));

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(output(s0), outputType(is(OutputType.VALUE)));
        assertThat(output(s0), outputGroup(is(nullValue())));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(s1, operationType(is(OperationType.EXTRACT)));
        assertThat(s1, not(operationOption(is(OperationOption.PRE_AGGREGATION))));
        assertThat(input(s1), inputType(is(InputType.EXTRACT)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), inputGroup(is(nullValue())));
        assertThat(output(s1), outputType(is(OutputType.VALUE)));
        assertThat(output(s1), outputGroup(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * co-group kind.
<pre>{@code
in --- o0 --- out
==>
in --- *G --- o0 --- *C --- out
}</pre>
     */
    @Test
    public void cogroup_kind() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in", DataSize.LARGE)
            .bless("o0", op(CoGroup.class, "cogroup")
                    .input("in", m.getCommonDataType(), group("=a"))
                    .output("out", m.getCommonDataType()))
                .connect("in", "o0")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(output(s0), outputType(is(OutputType.KEY_VALUE)));
        assertThat(output(s0), outputGroup(is(group("=a"))));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(info(s1).toString(), s1, primaryOperator(isOperator("o0")));
        assertThat(s1, operationType(is(OperationType.CO_GROUP)));
        assertThat(s1, not(operationOption(is(OperationOption.PRE_AGGREGATION))));
        assertThat(input(s1), inputType(is(InputType.CO_GROUP)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), not(inputOption(is(InputOption.SPILL_OUT))));
        assertThat(input(s1), inputGroup(is(group("=a"))));
        assertThat(output(s1), outputType(is(OutputType.VALUE)));
        assertThat(output(s1), outputGroup(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * co-group kind.
<pre>{@code
in --- o0 --- out
==>
in --- *G --- o0 --- *C --- out
}</pre>
     */
    @Test
    public void cogroup_spill_out() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
                .input("in", DataSize.LARGE)
                .bless("o0", op(CoGroup.class, "cogroup_escape")
                        .input("in", m.getCommonDataType(), group("=a"))
                        .output("out", m.getCommonDataType()))
                .connect("in", "o0")
                .output("out").connect("o0", "out")
                .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(output(s0), outputType(is(OutputType.KEY_VALUE)));
        assertThat(output(s0), outputGroup(is(group("=a"))));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(info(s1).toString(), s1, primaryOperator(isOperator("o0")));
        assertThat(s1, operationType(is(OperationType.CO_GROUP)));
        assertThat(s1, not(operationOption(is(OperationOption.PRE_AGGREGATION))));
        assertThat(input(s1), inputType(is(InputType.CO_GROUP)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), inputOption(is(InputOption.SPILL_OUT)));
        assertThat(input(s1), inputGroup(is(group("=a"))));
        assertThat(output(s1), outputType(is(OutputType.VALUE)));
        assertThat(output(s1), outputGroup(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * aggregate (total) kind.
<pre>{@code
in ---  o0 --- out
==>
in --- *G --- o0 --- *C --- out
}</pre>
     */
    @Test
    public void aggregate_kind_total() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in", DataSize.LARGE)
            .bless("o0", op(Fold.class, "fold_total")
                    .input("in", m.getCommonDataType(), group("=a"))
                    .output("out", m.getCommonDataType()))
                    .connect("in", "o0")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(output(s0), outputType(is(OutputType.KEY_VALUE)));
        assertThat(output(s0), outputGroup(is(group("=a"))));
        assertThat(output(s0), outputAggregation(isOperator("o0")));

        assertThat(info(s1).toString(), s1, primaryOperator(isOperator("o0")));
        assertThat(s1, operationType(is(OperationType.CO_GROUP)));
        assertThat(s1, not(operationOption(is(OperationOption.PRE_AGGREGATION))));
        assertThat(input(s1), inputType(is(InputType.CO_GROUP)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), not(inputOption(is(InputOption.SPILL_OUT))));
        assertThat(input(s1), inputGroup(is(group("=a"))));
        assertThat(output(s1), outputType(is(OutputType.VALUE)));
        assertThat(output(s1), outputGroup(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * aggregate (partial) kind.
<pre>{@code
in --- o0 --- out
==>
in --- *G --- o0 --- *C --- out
}</pre>
     */
    @Test
    public void aggregate_kind_partial() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
                .input("in", DataSize.LARGE)
                .bless("o0", op(Fold.class, "fold_partial")
                        .input("in", m.getCommonDataType(), group("=a"))
                        .output("out", m.getCommonDataType()))
                        .connect("in", "o0")
                .output("out").connect("o0", "out")
                .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(output(s0), outputType(is(OutputType.KEY_VALUE)));
        assertThat(output(s0), outputGroup(is(group("=a"))));
        assertThat(output(s0), outputAggregation(isOperator("o0")));

        assertThat(info(s1).toString(), s1, primaryOperator(isOperator("o0")));
        assertThat(s1, operationType(is(OperationType.CO_GROUP)));
        assertThat(s1, operationOption(is(OperationOption.PRE_AGGREGATION)));
        assertThat(input(s1), inputType(is(InputType.CO_GROUP)));
        assertThat(input(s1), inputOption(is(InputOption.PRIMARY)));
        assertThat(input(s1), not(inputOption(is(InputOption.SPILL_OUT))));
        assertThat(input(s1), inputGroup(is(group("=a"))));
        assertThat(output(s1), outputType(is(OutputType.VALUE)));
        assertThat(output(s1), outputGroup(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * with broadcast from different origins.
<pre>{@code
in0 --+ o0 --- out
in1 -/
==>
       in0 --+ o0 --- *C --- out
in1 --- *B -/
}</pre>
     */
    @Test
    public void broadcast() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in0", DataSize.LARGE)
            .input("in1", DataSize.TINY)
            .bless("o0", newJoin(m))
                .connect("in0", "o0.t")
                .connect("in1", "o0.m")
            .output("out").connect("o0.f", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));

        assertThat(info(s0).toString(), s0, primaryOperator(isOperator("in0")));
        assertThat(s0, operationType(is(OperationType.EXTRACT)));
        assertThat(s0, not(operationOption(is(OperationOption.PRE_AGGREGATION))));

        assertThat(output(s1), outputType(is(OutputType.BROADCAST)));
        assertThat(output(s1), outputGroup(nullValue()));
        assertThat(output(s1), outputAggregation(is(nullValue())));

        assertThat(output(s1).getOpposites(), hasSize(1));
        SubPlan.Input bIn = output(s1).getOpposites().iterator().next();
        assertThat(bIn, inputType(is(InputType.BROADCAST)));
        assertThat(bIn, not(inputOption(is(InputOption.PRIMARY))));
        assertThat(bIn, inputGroup(is(group("+k"))));
    }

    /**
     * with broadcast from same origin.
<pre>{@code
in +-----+ x0 --- out
    \   /
     +-+
==>

in +--- *C ---+ x0 --- *C --- out
    \        /
     +- *B -+
}</pre>
     */
    @Test
    public void broadcast_same_origin() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in", DataSize.TINY)
            .bless("x0", newJoin(m))
                .connect("in", "x0.t")
                .connect("in", "x0.m")
            .output("out").connect("x0.f", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("x0"));
        SubPlan s2 = ownerOf(detail, mock.get("out"));

        assertThat(pred(s0), is(empty()));
        assertThat(pred(s1), contains(s0));
        assertThat(pred(s2), contains(s1));

        assertThat(s0.getOutputs(), containsInAnyOrder(Arrays.asList(
                outputType(is(OutputType.VALUE)),
                outputType(is(OutputType.BROADCAST)))));

        assertThat(s1.getInputs(), containsInAnyOrder(Arrays.asList(
                inputType(is(InputType.EXTRACT)),
                inputType(is(InputType.BROADCAST)))));

    }

    /**
     * with broadcast from same origin that requires scatter-gather for its input.
<pre>{@code
in --- x0 +-----+ x1 --- out
           \   /
            +-+
==>

in --- *G --- x0 +--- *C ---+ x1 --- *C --- out
                  \        /
                   +- *B -+
}</pre>
     */
    @Test
    public void broadcast_self_join() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in", DataSize.TINY)
            .bless("x0", op(CoGroup.class, "cogroup")
                    .input("in", m.getCommonDataType(), group("=a"))
                    .output("out", m.getCommonDataType()))
                .connect("in", "x0")
            .bless("x1", op(Extract.class, "extract")
                    .input("t", m.getCommonDataType())
                    .input("m", m.getCommonDataType(), c -> c
                            .unit(InputUnit.WHOLE)
                            .group(group())
                            .attribute(ViewInfo.class, ViewInfo.flat()))
                    .output("out", m.getCommonDataType()))
                .connect("x0", "x1.t")
                .connect("x0", "x1.m")
            .output("out").connect("x1", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("x0"));
        SubPlan s2 = ownerOf(detail, mock.get("x1"));
        SubPlan s3 = ownerOf(detail, mock.get("out"));

        assertThat(pred(s0), is(empty()));
        assertThat(pred(s1), contains(s0));
        assertThat(pred(s2), contains(s1));
        assertThat(pred(s3), contains(s2));

        assertThat(s1.getOutputs(), containsInAnyOrder(Arrays.asList(
                outputType(is(OutputType.VALUE)),
                outputType(is(OutputType.BROADCAST)))));

        assertThat(s2.getInputs(), containsInAnyOrder(Arrays.asList(
                inputType(is(InputType.EXTRACT)),
                inputType(is(InputType.BROADCAST)))));
    }

    /**
     * with broadcast from cross origin.
<pre>{@code
in0 +---+ o0 --- out0
     \ /
      +
     / \
in1 +---+ o1 --- out1

==>

in0 +-- *C [C0]
     \
      +- *B --+
               \
        in1 +---+ x0 --- *C --- out1
             \
              \   [C0] ---+ x1 --- *C ---out2
               \         /
                +- *B --/
}</pre>
     */
    @Test
    public void broadcast_cross_origin() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
                .input("in0", DataSize.TINY)
                .input("in1", DataSize.TINY)
                .bless("x0", newJoin(m))
                .bless("x1", newJoin(m))
                .connect("in0", "x0.t")
                .connect("in0", "x1.m")
                .connect("in1", "x0.m")
                .connect("in1", "x1.t")
                .output("out0").connect("x0.f", "out0")
                .output("out1").connect("x1.f", "out1")
                .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(5));

        SubPlan si0 = ownerOf(detail, mock.get("in0"));
        SubPlan si1 = ownerOf(detail, mock.get("in1"));
        SubPlan sx0 = ownerOf(detail, mock.get("x0"));
        SubPlan sx1 = ownerOf(detail, mock.get("x1"));
        SubPlan so0 = ownerOf(detail, mock.get("out0"));
        SubPlan so1 = ownerOf(detail, mock.get("out1"));

        assertThat(si0, either(is(sx0)).or(isIn(pred(sx0))));
        assertThat(si0, isIn(pred(sx1)));
        assertThat(si1, either(is(sx1)).or(isIn(pred(sx1))));
        assertThat(si1, isIn(pred(sx0)));
        assertThat(sx0, isIn(pred(so0)));
        assertThat(sx1, isIn(pred(so1)));

        assertThat(si0.getOutputs(), containsInAnyOrder(Arrays.asList(
                outputType(is(OutputType.VALUE)),
                outputType(is(OutputType.BROADCAST)))));

        assertThat(si1.getOutputs(), containsInAnyOrder(Arrays.asList(
                outputType(is(OutputType.VALUE)),
                outputType(is(OutputType.BROADCAST)))));
    }

    /**
     * avoiding cyclic dependencies with broadcast.
<pre>{@code
in +------------+ x1 --- out
    \          /
     +-- x0 --+
==>

in ----- *C ------------------+ x1 --- *C --- out
    \                        /
     +-- *G --- x0 --- *B --+
}</pre>
     */
    @Test
    public void broadcast_dag_complex() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in", DataSize.TINY)
            .bless("x0", op(CoGroup.class, "cogroup")
                    .input("in", m.getCommonDataType(), group("=a"))
                    .output("out", m.getCommonDataType()))
                .connect("in", "x0")
            .bless("x1", op(Extract.class, "extract")
                    .input("t", m.getCommonDataType())
                    .input("m", m.getCommonDataType(), c -> c
                            .unit(InputUnit.WHOLE)
                            .group(group())
                            .attribute(ViewInfo.class, ViewInfo.flat()))
                    .output("out", m.getCommonDataType()))
                .connect("in", "x1.t")
                .connect("x0", "x1.m")
            .output("out").connect("x1", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("x0"));
        SubPlan s2 = ownerOf(detail, mock.get("x1"));
        SubPlan s3 = ownerOf(detail, mock.get("out"));

        assertThat(pred(s0), is(empty()));
        assertThat(pred(s1), containsInAnyOrder(s0));
        assertThat(pred(s2), containsInAnyOrder(s0, s1));
        assertThat(pred(s3), containsInAnyOrder(s2));

        assertThat(s0.getOutputs(), containsInAnyOrder(Arrays.asList(
                outputType(is(OutputType.VALUE)),
                outputType(is(OutputType.KEY_VALUE)))));

        assertThat(s1.getInputs(), contains(inputType(is(InputType.CO_GROUP))));
        assertThat(s1.getOutputs(), contains(outputType(is(OutputType.BROADCAST))));

        assertThat(s2.getInputs(), containsInAnyOrder(Arrays.asList(
                inputType(is(InputType.EXTRACT)),
                inputType(is(InputType.BROADCAST)))));
        assertThat(s2.getOutputs(), contains(outputType(is(OutputType.VALUE))));

        assertThat(s3.getInputs(), contains(inputType(is(InputType.EXTRACT))));
    }

    /**
     * multiple broadcast inputs.
<pre>{@code
in0 --+ o0 --+ o1 --+ o2 --- out
in1 -/      /      /
in2 -------/      /
in3 -------------/
==>
       in0 --+ o0 --+ o1 --+ o2 --- *C --- out
in1 --- *B -/      /      /
in2 --- *B -------/      /
in3 --- *B -------------/
}</pre>
     */
    @Test
    public void broadcast_chain() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in0", DataSize.LARGE)
            .input("in1", DataSize.TINY)
            .input("in2", DataSize.TINY)
            .input("in3", DataSize.TINY)
            .bless("o0", newJoin(m))
                .connect("in0", "o0.t")
                .connect("in1", "o0.m")
            .bless("o1", newJoin(m))
                .connect("o0.f", "o1.t")
                .connect("in2", "o1.m")
            .bless("o2", newJoin(m))
                .connect("o1.f", "o2.t")
                .connect("in3", "o2.m")
            .output("out").connect("o2.f", "out")
            .toGraph());
        MockOperators mock = restore(detail);

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(5));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        SubPlan s2 = ownerOf(detail, mock.get("in2"));
        SubPlan s3 = ownerOf(detail, mock.get("in3"));
        SubPlan s4 = ownerOf(detail, mock.get("out"));
        assertThat("each sub-plan is identical", set(s0, s1, s2, s3, s4), hasSize(5));

        assertThat(s0.getOperators(), hasOperators("in0", "in0", "o0", "o1", "o2"));
        assertThat(s1.getOperators(), hasOperators("in1"));
        assertThat(s2.getOperators(), hasOperators("in2"));
        assertThat(s3.getOperators(), hasOperators("in3"));
        assertThat(s4.getOperators(), hasOperators("out"));
    }

    /**
     * co-group w/ data table.
<pre>{@code
in0 --+ o0 --- out
in1 -/
==>
        in0 --+- o0 --- *C --- out
             /
in1 --- *B -/
}</pre>
     */
    @Test
    public void extract_with_datatable() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
                .input("in0", DataSize.LARGE)
                .input("in1", DataSize.LARGE)
                .bless("o0", op(Extract.class, "extract")
                        .input("in", m.getCommonDataType(), c -> c
                                .unit(InputUnit.RECORD))
                        .input("side", m.getCommonDataType(), c -> c
                                .unit(InputUnit.WHOLE)
                                .group(group("=k")))
                        .output("out", m.getCommonDataType()))
                .connect("in0", "o0.in")
                .connect("in1", "o0.side")
                .output("out").connect("o0", "out")
                .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));

        assertThat(info(s0).toString(), s0, primaryOperator(isOperator("in0")));
        assertThat(s0, operationType(is(OperationType.EXTRACT)));
        assertThat(s0, operationOption(is(OperationOption.EXTERNAL_INPUT)));
        assertThat(secondary(s0), inputType(is(InputType.BROADCAST)));
        assertThat(secondary(s0), not(inputOption(is(InputOption.PRIMARY))));
        assertThat(secondary(s0), inputGroup(is(group("=k"))));

        assertThat(s1, operationOption(is(OperationOption.EXTERNAL_INPUT)));
        assertThat(output(s1), outputType(is(OutputType.BROADCAST)));
        assertThat(output(s1), outputGroup(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));
    }

    /**
     * co-group w/ data table.
<pre>{@code
in0 --+ o0 --- out
in1 -/
==>
in0 --- *G ---+- o0 --- *C --- out
             /
in1 --- *B -/
}</pre>
     */
    @Test
    public void cogroup_with_datatable() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in0", DataSize.LARGE)
            .input("in1", DataSize.LARGE)
            .bless("o0", op(CoGroup.class, "cogroup")
                    .input("in", m.getCommonDataType(), c -> c
                            .unit(InputUnit.GROUP)
                            .group(group("=a")))
                    .input("side", m.getCommonDataType(), c -> c
                            .unit(InputUnit.WHOLE)
                            .group(group("=b")))
                    .output("out", m.getCommonDataType()))
                .connect("in0", "o0.in")
                .connect("in1", "o0.side")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        SubPlan s2 = ownerOf(detail, mock.get("o0"));

        assertThat(s0, operationOption(is(OperationOption.EXTERNAL_INPUT)));
        assertThat(output(s0), outputType(is(OutputType.KEY_VALUE)));
        assertThat(output(s0), outputGroup(is(group("=a"))));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(s1, operationOption(is(OperationOption.EXTERNAL_INPUT)));
        assertThat(output(s1), outputType(is(OutputType.BROADCAST)));
        assertThat(output(s1), outputGroup(is(nullValue())));
        assertThat(output(s1), outputAggregation(is(nullValue())));

        assertThat(info(s2).toString(), s2, primaryOperator(isOperator("o0")));
        assertThat(s2, operationType(is(OperationType.CO_GROUP)));
        assertThat(s2, not(operationOption(is(OperationOption.PRE_AGGREGATION))));
        assertThat(primary(s2), inputType(is(InputType.CO_GROUP)));
        assertThat(primary(s2), inputOption(is(InputOption.PRIMARY)));
        assertThat(primary(s2), not(inputOption(is(InputOption.SPILL_OUT))));
        assertThat(primary(s2), inputGroup(is(group("=a"))));
        assertThat(secondary(s2), inputType(is(InputType.BROADCAST)));
        assertThat(secondary(s2), not(inputOption(is(InputOption.PRIMARY))));
        assertThat(secondary(s2), inputGroup(is(group("=b"))));
    }

    /**
     * through kind.
<pre>{@code
in --- c0 --- o0 --- out
==>
in --- *C --- *G --- o0 --- *C --- out
          ~~~ <- through
}</pre>
     */
    @Test
    public void through() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in")
            .operator(cp(), "c0").connect("in", "c0")
            .bless("o0", op(CoGroup.class, "cogroup")
                    .input("in", m.getCommonDataType(), group("=a"))
                    .output("out", m.getCommonDataType()))
                    .connect("c0", "o0")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = succ(s0).iterator().next();
        SubPlan s2 = ownerOf(detail, mock.get("o0"));
        assertThat(s1, is(not(s2)));

        assertThat(output(s0), outputType(is(OutputType.VALUE)));
        assertThat(output(s0), outputGroup(is(nullValue())));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(info(s1).toString(), s1, operationType(is(OperationType.EXTRACT)));
        assertThat(s1, primaryOperator(nullValue()));
    }

    /**
     * co-group with several inputs are open.
     */
    @Test
    public void cogroup_partial() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in")
            .bless("o0", op(CoGroup.class, "cogroup")
                    .input("a", m.getCommonDataType(), group("=k"))
                    .input("b", m.getCommonDataType(), group("=k"))
                    .output("out", m.getCommonDataType()))
                .connect("in", "o0.a")
            .output("out").connect("o0", "out")
            .toGraph());
        MockOperators mock = restore(detail);

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));

        assertThat(s0.getInputs(), hasSize(1));
        assertThat(output(s0), outputType(is(OutputType.KEY_VALUE)));
        assertThat(output(s0), outputGroup(is(group("=k"))));
        assertThat(output(s0), outputAggregation(is(nullValue())));

        assertThat(s1.getInputs(), hasSize(1));
    }

    /**
     * join with broadcast input is open.
     */
    @Test
    public void broadcast_partial() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in0", DataSize.LARGE)
            .bless("o0", newJoin(m))
                .connect("in0", "o0.t")
            .output("out").connect("o0.f", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(2));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        assertThat(s0.getInputs(), hasSize(1));
    }

    /**
     * sub-plan w/ multiple dominant operators.
<pre>{@code
in --- c0 +--- o0 ---+ out
           \-- o1 --/
            \- o2 -/
==>
in --- *C +--- o0 ---+ *C --- out
           \-- o1 --/
            \- o2 -/
}</pre>
     */
    @Test
    public void multiple_dominant_operators() {
        PlanDetail detail = DagPlanning.plan(context(), new MockOperators()
            .input("in")
            .operator(cp(), "c0").connect("in", "c0")
            .operator(op(Extract.class, "extract"), "o0").connect("c0", "o0")
            .operator(op(Extract.class, "extract"), "o1").connect("c0", "o1")
            .operator(op(Extract.class, "extract"), "o2").connect("c0", "o2")
            .output("out")
                .connect("o0", "out")
                .connect("o1", "out")
                .connect("o2", "out")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));
        SubPlan s2 = ownerOf(detail, mock.get("out"));

        assertThat(info(s0).toString(), s0.getOperators(), hasOperators("in"));
        assertThat(info(s1).toString(), s1.getOperators(), hasOperators("o0", "o1", "o2"));
        assertThat(info(s2).toString(), s2.getOperators(), hasOperators("out"));
    }

    /**
     * each sub-plans should be unified with a focus on the inputs, and they are individual.
     */
    @Test
    public void unify_focus_input() {
        PlanDetail detail = DagPlanning.plan(context(), new MockOperators()
            .input("in0")
            .input("in1")
            .input("in2")
            .operator(op(Extract.class, "extract"), "o0")
                .connect("in0", "o0")
                .connect("in1", "o0")
                .connect("in2", "o0")
            .output("out0").connect("o0", "out0")
            .output("out1").connect("o0", "out1")
            .output("out2").connect("o0", "out2")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(6));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        SubPlan s2 = ownerOf(detail, mock.get("in2"));
        SubPlan s3 = ownerOf(detail, mock.get("out0"));
        SubPlan s4 = ownerOf(detail, mock.get("out1"));
        SubPlan s5 = ownerOf(detail, mock.get("out2"));
        assertThat("each sub-plan is identical", set(s0, s1, s2, s3, s4, s5), hasSize(6));

        assertThat(s0.getOperators(), hasOperators("in0", "o0"));
        assertThat(s1.getOperators(), hasOperators("in1", "o0"));
        assertThat(s2.getOperators(), hasOperators("in2", "o0"));
        assertThat(s3.getOperators(), hasOperators("out0"));
        assertThat(s4.getOperators(), hasOperators("out1"));
        assertThat(s5.getOperators(), hasOperators("out2"));
    }

    /**
     * each sub-plans should be unified with a focus on the inputs, and they are individual.
     */
    @Test
    public void unify_focus_input_complex() {
        PlanDetail detail = DagPlanning.plan(context(), new MockOperators()
            .input("in0")
            .input("in1")
            .input("in2")
            .operator(op(Extract.class, "extract"), "o0")
                .connect("in0", "o0")
                .connect("in1", "o0")
                .connect("in2", "o0")
            .operator(op(Extract.class, "extract"), "o1")
                .connect("in0", "o1")
                .connect("in1", "o1")
                .connect("in2", "o1")
            .operator(op(Extract.class, "extract"), "o2")
                .connect("in0", "o2")
                .connect("in1", "o2")
                .connect("in2", "o2")
            .output("out0")
                .connect("o0", "out0")
                .connect("o1", "out0")
                .connect("o2", "out0")
            .output("out1")
                .connect("o0", "out1")
                .connect("o1", "out1")
                .connect("o2", "out1")
            .output("out2")
                .connect("o0", "out2")
                .connect("o1", "out2")
                .connect("o2", "out2")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(6));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        SubPlan s2 = ownerOf(detail, mock.get("in2"));
        SubPlan s3 = ownerOf(detail, mock.get("out0"));
        SubPlan s4 = ownerOf(detail, mock.get("out1"));
        SubPlan s5 = ownerOf(detail, mock.get("out2"));
        assertThat("each sub-plan is identical", set(s0, s1, s2, s3, s4, s5), hasSize(6));

        assertThat(s0.getOperators(), hasOperators("in0", "o0", "o1", "o2"));
        assertThat(s1.getOperators(), hasOperators("in1", "o0", "o1", "o2"));
        assertThat(s2.getOperators(), hasOperators("in2", "o0", "o1", "o2"));
        assertThat(s3.getOperators(), hasOperators("out0"));
        assertThat(s4.getOperators(), hasOperators("out1"));
        assertThat(s5.getOperators(), hasOperators("out2"));
    }

    /**
     * equivalent sub-plan outputs (checkpoint) should be unified.
<pre>{@code
in --- o0 +--- out0
           \-- out1
            \- out2
}</pre>
     */
    @Test
    public void unify_subplan_output_checkpoint() {
        PlanDetail detail = DagPlanning.plan(context(), new MockOperators()
            .input("in")
            .operator(op(Extract.class, "extract"), "o0").connect("in", "o0")
            .output("out0").connect("o0", "out0")
            .output("out1").connect("o0", "out1")
            .output("out2").connect("o0", "out2")
            .toGraph());
        MockOperators mock = restore(detail);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("in"));
        assertThat(s0.getOutputs(), hasSize(1));
    }

    /**
     * equivalent sub-plan outputs (broadcast) should be unified.
<pre>{@code
in0 --+ o0 --+ o1 --+ o2 --- out
in1 -/------/------/
}</pre>
     */
    @Test
    public void unify_subplan_output_broadcast() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in0", DataSize.LARGE)
            .input("in1", DataSize.TINY)
            .bless("o0", newJoin(m))
                .connect("in0", "o0.t")
                .connect("in1", "o0.m")
            .bless("o1", newJoin(m))
                .connect("o0.f", "o1.t")
                .connect("in1", "o1.m")
            .bless("o2", newJoin(m))
                .connect("o1.f", "o2.t")
                .connect("in1", "o2.m")
            .output("out").connect("o2.f", "out")
            .toGraph());
        MockOperators mock = restore(detail);

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        assertThat(s0.getInputs(), hasSize(2));
        assertThat(s1.getOutputs(), hasSize(1));
    }

    /**
     * equivalent sub-plan outputs (gather) should be unified.
     */
    @Test
    public void unify_subplan_output_gather() {
        MockOperators m = new MockOperators();
        PlanDetail detail = DagPlanning.plan(context(), m
            .input("in0", DataSize.LARGE)
            .input("in1", DataSize.LARGE)
            .bless("o0", op(CoGroup.class, "cogroup")
                    .input("a", m.getCommonDataType(), group("=k0", "+k1"))
                    .input("b", m.getCommonDataType(), group("=k0", "-k1"))
                    .output("out", m.getCommonDataType()))
                .connect("in0", "o0.a")
                .connect("in1", "o0.b")
            .bless("o1", op(CoGroup.class, "cogroup")
                    .input("a", m.getCommonDataType(), group("=k0", "+k1"))
                    .input("b", m.getCommonDataType(), group("=k0", "-k1"))
                    .output("out", m.getCommonDataType()))
                .connect("in0", "o1.a")
                .connect("in1", "o1.b")
            .output("out")
                .connect("o0", "out")
                .connect("o1", "out")
            .toGraph());
        MockOperators mock = restore(detail);

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(5));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("in1"));
        SubPlan s2 = ownerOf(detail, mock.get("o0"));
        SubPlan s3 = ownerOf(detail, mock.get("o1"));

        assertThat(s0.getOutputs(), hasSize(1));
        assertThat(s1.getOutputs(), hasSize(1));
        assertThat(s2.getInputs(), hasSize(2));
        assertThat(s3.getInputs(), hasSize(2));
    }

    static Matcher<? super SubPlan> operationType(Matcher<? super OperationType> matcher) {
        return new FeatureMatcher<SubPlan, OperationType>(matcher, "driver type", "driver type") {
            @Override
            protected OperationType featureValueOf(SubPlan actual) {
                return info(actual).getOperationType();
            }
        };
    }

    static Matcher<? super SubPlan> operationOption(Matcher<? super OperationOption> matcher) {
        return new FeatureMatcher<SubPlan, Set<OperationOption>>(hasItem(matcher), "driver option", "driver option") {
            @Override
            protected Set<OperationOption> featureValueOf(SubPlan actual) {
                return info(actual).getOperationOptions();
            }
        };
    }

    static Matcher<? super SubPlan> primaryOperator(Matcher<? super Operator> matcher) {
        return new FeatureMatcher<SubPlan, Operator>(matcher, "primary operator", "primary operator") {
            @Override
            protected Operator featureValueOf(SubPlan actual) {
                return info(actual).getPrimaryOperator();
            }
        };
    }

    static Matcher<? super SubPlan.Input> inputType(Matcher<? super InputType> matcher) {
        return new FeatureMatcher<SubPlan.Input, InputType>(matcher, "input type", "input type") {
            @Override
            protected InputType featureValueOf(SubPlan.Input actual) {
                return info(actual).getInputType();
            }
        };
    }

    static Matcher<? super SubPlan.Input> inputOption(Matcher<? super InputOption> matcher) {
        return new FeatureMatcher<SubPlan.Input, Set<InputOption>>(hasItem(matcher), "input option", "input option") {
            @Override
            protected Set<InputOption> featureValueOf(SubPlan.Input actual) {
                return info(actual).getInputOptions();
            }
        };
    }

    static Matcher<? super SubPlan.Input> inputGroup(Matcher<? super Group> matcher) {
        return new FeatureMatcher<SubPlan.Input, Group>(matcher, "partition", "partition") {
            @Override
            protected Group featureValueOf(SubPlan.Input actual) {
                return info(actual).getPartitionInfo();
            }
        };
    }

    static Matcher<? super SubPlan.Output> outputType(Matcher<? super OutputType> matcher) {
        return new FeatureMatcher<SubPlan.Output, OutputType>(matcher, "output type", "output type") {
            @Override
            protected OutputType featureValueOf(SubPlan.Output actual) {
                return info(actual).getOutputType();
            }
        };
    }

    static Matcher<? super SubPlan.Output> outputOption(Matcher<? super OutputOption> matcher) {
        return new FeatureMatcher<SubPlan.Output, Set<OutputOption>>(hasItem(matcher), "output option", "output option") {
            @Override
            protected Set<OutputOption> featureValueOf(SubPlan.Output actual) {
                return info(actual).getOutputOptions();
            }
        };
    }

    static Matcher<? super SubPlan.Output> outputGroup(Matcher<? super Group> matcher) {
        return new FeatureMatcher<SubPlan.Output, Group>(matcher, "partition", "partition") {
            @Override
            protected Group featureValueOf(SubPlan.Output actual) {
                return info(actual).getPartitionInfo();
            }
        };
    }

    static Matcher<? super SubPlan.Output> outputAggregation(Matcher<? super Operator> matcher) {
        return new FeatureMatcher<SubPlan.Output, Operator>(matcher, "aggregator", "aggregator") {
            @Override
            protected Operator featureValueOf(SubPlan.Output actual) {
                return info(actual).getAggregationInfo();
            }
        };
    }

    private static CoreOperator.Builder cp() {
        return CoreOperator.builder(CoreOperatorKind.CHECKPOINT);
    }

    private static UserOperator.Builder op(Class<? extends Annotation> annotation, String name) {
        return OperatorExtractor.extract(annotation, Ops.class, name);
    }

    private UserOperator.Builder newJoin(MockOperators m) {
        return op(MasterJoinUpdate.class, "join")
                .input("m", m.getCommonDataType(), group("+k"))
                .input("t", m.getCommonDataType(), group("+k"))
                .output("f", m.getCommonDataType())
                .output("m", m.getCommonDataType());
    }

    @SuppressWarnings("javadoc")
    public abstract static class Ops {

        @Extract
        public abstract void extract();

        @CoGroup
        public abstract void cogroup();

        @CoGroup(inputBuffer = InputBuffer.ESCAPE)
        public abstract void cogroup_escape();

        @Fold(partialAggregation = PartialAggregation.PARTIAL)
        public abstract void fold_partial();

        @Fold(partialAggregation = PartialAggregation.TOTAL)
        public abstract void fold_total();

        @MasterJoinUpdate
        public abstract void join();
    }
}
