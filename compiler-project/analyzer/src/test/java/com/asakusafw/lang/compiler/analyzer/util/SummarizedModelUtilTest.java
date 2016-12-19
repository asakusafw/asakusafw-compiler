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
package com.asakusafw.lang.compiler.analyzer.util;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.vocabulary.model.Key;
import com.asakusafw.vocabulary.model.Summarized;
import com.asakusafw.vocabulary.operator.Summarize;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Test for {@link SummarizedModelUtil}.
 */
public class SummarizedModelUtilTest {

    private final ClassLoader cl = getClass().getClassLoader();

    /**
     * simple case for master join operator.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        Operator operator = newJoin();
        assertThat(SummarizedModelUtil.isSupported(operator), is(true));

        List<PropertyFolding> foldings = SummarizedModelUtil.getPropertyFoldings(cl, operator);
        assertThat(foldings, hasSize(3));

        OperatorInput input = operator.getInput(0);
        OperatorOutput output = operator.getOutput(0);
        PropertyFolding fk = find("k", foldings);
        PropertyFolding fv = find("v", foldings);
        PropertyFolding fc = find("c", foldings);

        assertThat(fk.getMapping().getSourcePort(), is(input));
        assertThat(fv.getMapping().getSourcePort(), is(input));
        assertThat(fc.getMapping().getSourcePort(), is(input));

        assertThat(fk.getMapping().getDestinationPort(), is(output));
        assertThat(fv.getMapping().getDestinationPort(), is(output));
        assertThat(fc.getMapping().getDestinationPort(), is(output));

        assertThat(fk.getMapping().getSourceProperty(), is(PropertyName.of("k0")));
        assertThat(fv.getMapping().getSourceProperty(), is(PropertyName.of("v0")));
        assertThat(fc.getMapping().getSourceProperty(), is(PropertyName.of("v0")));

        assertThat(fk.getAggregation(), is(PropertyFolding.Aggregation.ANY));
        assertThat(fv.getAggregation(), is(PropertyFolding.Aggregation.SUM));
        assertThat(fc.getAggregation(), is(PropertyFolding.Aggregation.COUNT));
    }

    /**
     * supported.
     */
    @Test
    public void supported_model() {
        assertThat(SummarizedModelUtil.isSupported(Left.class), is(false));
        assertThat(SummarizedModelUtil.isSupported(S0.class), is(true));
    }

    /**
     * supported.
     */
    @Test
    public void supported_operator() {
        Operator m0 = newJoin();
        Operator m1 = newUpdate();
        CoreOperator m2 = newCheckpoint();

        assertThat(SummarizedModelUtil.isSupported(m0), is(true));
        assertThat(SummarizedModelUtil.isSupported(m1), is(false));
        assertThat(SummarizedModelUtil.isSupported(m2), is(false));
    }

    /**
     * unsupported operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_unsupported() throws Exception {
        SummarizedModelUtil.getPropertyFoldings(cl, newCheckpoint());
    }

    /**
     * unsupported operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_not_joined() throws Exception {
        SummarizedModelUtil.getPropertyFoldings(cl, newInvalid());
    }

    private PropertyFolding find(String destination, List<PropertyFolding> foldings) {
        PropertyName name = PropertyName.of(destination);
        for (PropertyFolding folding : foldings) {
            if (folding.getMapping().getDestinationProperty().equals(name)) {
                return folding;
            }
        }
        throw new AssertionError(destination);
    }

    private Operator newUpdate() {
        return OperatorExtractor.extract(Update.class, Ops.class, "update")
                .input("in", classOf(S0.class))
                .output("out", classOf(S0.class))
                .build();
    }

    private Operator newJoin() {
        return OperatorExtractor.extract(Summarize.class, Ops.class, "summarize")
                .input("in", classOf(Left.class))
                .output("out", classOf(S0.class))
                .build();
    }

    private Operator newInvalid() {
        return OperatorExtractor.extract(Summarize.class, Ops.class, "invalid")
                .input("in", classOf(Left.class))
                .output("out", classOf(String.class))
                .build();
    }

    private CoreOperator newCheckpoint() {
        return CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build();
    }

    private static class Left {
        // no members
    }

    @Summarized(term = @Summarized.Term(source = Left.class, shuffle = @Key(group = {}), foldings = {
        @Summarized.Folding(source = "k0", destination = "k", aggregator = Summarized.Aggregator.ANY),
        @Summarized.Folding(source = "v0", destination = "v", aggregator = Summarized.Aggregator.SUM),
        @Summarized.Folding(source = "v0", destination = "c", aggregator = Summarized.Aggregator.COUNT),
    }))
    private static class S0 {
        // no members
    }

    private abstract static class Ops {

        @Update
        public void update(S0 model) {
            return;
        }

        @Summarize
        public abstract S0 summarize(Left left);

        @Summarize
        public abstract String invalid(Left left);
    }
}
