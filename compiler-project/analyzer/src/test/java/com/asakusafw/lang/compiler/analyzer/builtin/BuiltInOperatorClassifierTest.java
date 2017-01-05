/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.analyzer.builtin;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.Groups;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo.DataSize;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputAttribute;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputType;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.attribute.BufferType;
import com.asakusafw.vocabulary.flow.processor.InputBuffer;
import com.asakusafw.vocabulary.flow.processor.PartialAggregation;
import com.asakusafw.vocabulary.model.Once;
import com.asakusafw.vocabulary.model.Spill;
import com.asakusafw.vocabulary.operator.CoGroup;
import com.asakusafw.vocabulary.operator.Extract;
import com.asakusafw.vocabulary.operator.Fold;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;

/**
 * Test for {@link BuiltInOperatorClassifier}.
 */
public class BuiltInOperatorClassifierTest extends BuiltInOptimizerTestRoot {

    /**
     * test for extract kind.
     */
    @Test
    public void extract() {
        Operator operator = OperatorExtractor.extract(Extract.class, Ops.class, "extract")
                .input("p", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
        OperatorClass result = apply(context(), operator, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.RECORD));
        assertThat(result.getPrimaryInputs(), hasSize(1));
        assertThat(result.getSecondaryInputs(), hasSize(0));
    }

    /**
     * test for co-group kind.
     */
    @Test
    public void cogroup() {
        Operator operator = OperatorExtractor.extract(CoGroup.class, Ops.class, "cogroup")
                .input("a", typeOf(String.class), Groups.parse(list(), list("+p")))
                .input("b", typeOf(String.class), Groups.parse(list(), list()))
                .output("p", typeOf(String.class))
                .build();
        OperatorClass result = apply(context(), operator, DataSize.UNKNOWN, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.GROUP));
        assertThat(result.getPrimaryInputs(), hasSize(2));
        assertThat(result.getSecondaryInputs(), hasSize(0));

        OperatorInput i0 = operator.getInput(0);
        assertThat(result.getAttributes(i0), hasItem(InputAttribute.SORTED));
        assertThat(result.getAttributes(i0), not(hasItem(InputAttribute.ESCAPED)));
        assertThat(result.getAttributes(i0), not(hasItem(InputAttribute.AGGREATE)));
        assertThat(result.getAttributes(i0), not(hasItem(InputAttribute.PARTIAL_REDUCTION)));

        OperatorInput i1 = operator.getInput(1);
        assertThat(result.getAttributes(i1), not(hasItem(InputAttribute.SORTED)));
        assertThat(result.getAttributes(i1), not(hasItem(InputAttribute.ESCAPED)));
        assertThat(result.getAttributes(i1), not(hasItem(InputAttribute.AGGREATE)));
        assertThat(result.getAttributes(i1), not(hasItem(InputAttribute.PARTIAL_REDUCTION)));
    }

    /**
     * test for co-group kind w/ inputbuffer=escape.
     */
    @Test
    public void cogroup_escape() {
        Operator operator = OperatorExtractor.extract(CoGroup.class, Ops.class, "cogroup_escape")
                .input("a", typeOf(String.class), Groups.parse(list(), list("+p")))
                .input("b", typeOf(String.class), Groups.parse(list(), list()))
                .output("p", typeOf(String.class))
                .build();
        OperatorClass result = apply(context(), operator, DataSize.UNKNOWN, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.GROUP));
        assertThat(result.getPrimaryInputs(), hasSize(2));
        assertThat(result.getSecondaryInputs(), hasSize(0));

        OperatorInput i0 = operator.getInput(0);
        assertThat(result.getAttributes(i0), hasItem(InputAttribute.SORTED));
        assertThat(result.getAttributes(i0), hasItem(InputAttribute.ESCAPED));
        assertThat(result.getAttributes(i0), not(hasItem(InputAttribute.AGGREATE)));
        assertThat(result.getAttributes(i0), not(hasItem(InputAttribute.PARTIAL_REDUCTION)));

        OperatorInput i1 = operator.getInput(1);
        assertThat(result.getAttributes(i1), not(hasItem(InputAttribute.SORTED)));
        assertThat(result.getAttributes(i1), hasItem(InputAttribute.ESCAPED));
        assertThat(result.getAttributes(i1), not(hasItem(InputAttribute.AGGREATE)));
        assertThat(result.getAttributes(i1), not(hasItem(InputAttribute.PARTIAL_REDUCTION)));
    }

    /**
     * test for co-group kind w/ buffer types.
     */
    @Test
    public void cogroup_buffer() {
        Operator operator = OperatorExtractor.extract(CoGroup.class, Ops.class, "cogroup_buffer")
                .input("a", typeOf(String.class), c -> c
                        .group(Groups.parse(list(), list()))
                        .attribute(BufferType.HEAP))
                .input("a", typeOf(String.class), c -> c
                        .group(Groups.parse(list(), list()))
                        .attribute(BufferType.SPILL))
                .input("a", typeOf(String.class), c -> c
                        .group(Groups.parse(list(), list()))
                        .attribute(BufferType.VOLATILE))
                .output("p", typeOf(String.class))
                .build();
        OperatorClass result = apply(context(), operator, DataSize.UNKNOWN, DataSize.UNKNOWN, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.GROUP));
        assertThat(result.getPrimaryInputs(), hasSize(3));
        assertThat(result.getSecondaryInputs(), hasSize(0));

        OperatorInput i0 = operator.getInput(0);
        assertThat(result.getAttributes(i0), not(hasItem(InputAttribute.ESCAPED)));
        assertThat(result.getAttributes(i0), not(hasItem(InputAttribute.VOALTILE)));

        OperatorInput i1 = operator.getInput(1);
        assertThat(result.getAttributes(i1), hasItem(InputAttribute.ESCAPED));
        assertThat(result.getAttributes(i1), not(hasItem(InputAttribute.VOALTILE)));

        OperatorInput i2 = operator.getInput(2);
        assertThat(result.getAttributes(i2), not(hasItem(InputAttribute.ESCAPED)));
        assertThat(result.getAttributes(i2), hasItem(InputAttribute.VOALTILE));
    }

    /**
     * test for join.
     */
    @Test
    public void join_unknown() {
        Operator operator = OperatorExtractor.extract(MasterJoinUpdate.class, Ops.class, "join")
                .input("m", typeOf(String.class))
                .input("t", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
        OperatorClass result = apply(context(), operator, DataSize.UNKNOWN, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.GROUP));
        assertThat(result.getPrimaryInputs(), hasSize(2));
        assertThat(result.getSecondaryInputs(), hasSize(0));
    }

    /**
     * test for join.
     */
    @Test
    public void join_tiny() {
        Operator operator = OperatorExtractor.extract(MasterJoinUpdate.class, Ops.class, "join")
                .input("m", typeOf(String.class))
                .input("t", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
        OperatorClass result = apply(context(), operator, DataSize.TINY, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.RECORD));
        assertThat(result.getPrimaryInputs(), contains(operator.getInput(1)));
        assertThat(result.getSecondaryInputs(), contains(operator.getInput(0)));
    }

    /**
     * test for join.
     */
    @Test
    public void join_small() {
        Operator operator = OperatorExtractor.extract(MasterJoinUpdate.class, Ops.class, "join")
                .input("m", typeOf(String.class))
                .input("t", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
        OperatorClass result = apply(context(), operator, DataSize.SMALL, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.GROUP));
        assertThat(result.getPrimaryInputs(), hasSize(2));
        assertThat(result.getSecondaryInputs(), hasSize(0));
    }

    /**
     * test for join.
     */
    @Test
    public void join_custom() {
        Operator operator = OperatorExtractor.extract(MasterJoinUpdate.class, Ops.class, "join")
                .input("m", typeOf(String.class))
                .input("t", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
        OptimizerContext context = context(
                MasterJoinOperatorClassifier.KEY_BROADCAST_LIMIT,
                String.valueOf(1L << 40));
        OperatorClass result = apply(context, operator, DataSize.SMALL, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.RECORD));
        assertThat(result.getPrimaryInputs(), contains(operator.getInput(1)));
        assertThat(result.getSecondaryInputs(), contains(operator.getInput(0)));
    }

    /**
     * test for aggregate kind.
     */
    @Test
    public void aggregate() {
        Operator operator = OperatorExtractor.extract(Fold.class, Ops.class, "aggregate")
                .input("p", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
        OperatorClass result = apply(context(), operator, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.GROUP));
        assertThat(result.getPrimaryInputs(), hasSize(1));
        assertThat(result.getSecondaryInputs(), hasSize(0));

        OperatorInput input = operator.getInput(0);
        assertThat(result.getAttributes(input), hasItem(InputAttribute.AGGREATE));
        assertThat(result.getAttributes(input), not(hasItem(InputAttribute.PARTIAL_REDUCTION)));
    }

    /**
     * test for aggregate kind.
     */
    @Test
    public void aggregate_custom() {
        Operator operator = OperatorExtractor.extract(Fold.class, Ops.class, "aggregate")
                .input("p", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
        OptimizerContext context = context(AggregationOperatorClassifier.KEY_AGGREGATION, "partial");
        OperatorClass result = apply(context, operator, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.GROUP));
        assertThat(result.getPrimaryInputs(), hasSize(1));
        assertThat(result.getSecondaryInputs(), hasSize(0));

        OperatorInput input = operator.getInput(0);
        assertThat(result.getAttributes(input), hasItem(InputAttribute.AGGREATE));
        assertThat(result.getAttributes(input), hasItem(InputAttribute.PARTIAL_REDUCTION));
    }

    /**
     * test for aggregate kind.
     */
    @Test
    public void aggregate_total() {
        Operator operator = OperatorExtractor.extract(Fold.class, Ops.class, "aggregate_total")
                .input("p", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
        OperatorClass result = apply(context(), operator, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.GROUP));
        assertThat(result.getPrimaryInputs(), hasSize(1));
        assertThat(result.getSecondaryInputs(), hasSize(0));

        OperatorInput input = operator.getInput(0);
        assertThat(result.getAttributes(input), hasItem(InputAttribute.AGGREATE));
        assertThat(result.getAttributes(input), not(hasItem(InputAttribute.PARTIAL_REDUCTION)));
    }

    /**
     * test for aggregate kind.
     */
    @Test
    public void aggregate_partial() {
        Operator operator = OperatorExtractor.extract(Fold.class, Ops.class, "aggregate_partial")
                .input("p", typeOf(String.class))
                .output("p", typeOf(String.class))
                .build();
        OperatorClass result = apply(context(), operator, DataSize.UNKNOWN);
        assertThat(result.getOperator(), is(operator));
        assertThat(result.getPrimaryInputType(), is(InputType.GROUP));
        assertThat(result.getPrimaryInputs(), hasSize(1));
        assertThat(result.getSecondaryInputs(), hasSize(0));

        OperatorInput input = operator.getInput(0);
        assertThat(result.getAttributes(input), hasItem(InputAttribute.AGGREATE));
        assertThat(result.getAttributes(input), hasItem(InputAttribute.PARTIAL_REDUCTION));
    }

    private OperatorClass apply(OptimizerContext context, Operator operator, DataSize... inputSizes) {
        List<OperatorInput> inputs = operator.getInputs();
        assertThat(inputs, hasSize(inputSizes.length));
        for (int i = 0; i < inputSizes.length; i++) {
            OperatorInput input = inputs.get(i);
            String name = "in" + i;
            ExternalInput upstream = ExternalInput.newInstance(
                    name,
                    new ExternalInputInfo.Basic(
                            new ClassDescription(name),
                            name,
                            (ClassDescription) input.getDataType(),
                            inputSizes[i]));
            input.connect(upstream.getOperatorPort());
        }
        return apply(context, new BuiltInOperatorClassifier(), operator);
    }

    private static List<String> list(String... args) {
        return Arrays.asList(args);
    }

    @SuppressWarnings("javadoc")
    public abstract static class Ops {

        @Extract
        public abstract void extract(String i, Result<String> r);

        @CoGroup
        public abstract void cogroup(List<String> a, List<String> b, Result<String> r);

        @CoGroup(inputBuffer = InputBuffer.ESCAPE)
        public abstract void cogroup_escape(List<String> a, List<String> b, Result<String> r);

        @CoGroup
        public abstract void cogroup_buffer(
                List<String> a,
                @Spill List<String> b,
                @Once Iterable<String> c,
                Result<String> r);

        @MasterJoinUpdate
        public abstract void join(String m, String t);

        @Fold(partialAggregation = PartialAggregation.DEFAULT)
        public abstract void aggregate(String a, String b);

        @Fold(partialAggregation = PartialAggregation.PARTIAL)
        public abstract void aggregate_partial(String a, String b);

        @Fold(partialAggregation = PartialAggregation.TOTAL)
        public abstract void aggregate_total(String a, String b);
    }
}
