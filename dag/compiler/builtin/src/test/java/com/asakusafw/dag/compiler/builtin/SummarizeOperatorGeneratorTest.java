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
package com.asakusafw.dag.compiler.builtin;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.function.Function;

import org.junit.Test;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.AggregateNodeInfo;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.runtime.adapter.ObjectCopier;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockSink;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Groups;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.vocabulary.model.Key;
import com.asakusafw.vocabulary.model.Summarized;
import com.asakusafw.vocabulary.model.Summarized.Aggregator;
import com.asakusafw.vocabulary.model.Summarized.Folding;
import com.asakusafw.vocabulary.model.Summarized.Term;
import com.asakusafw.vocabulary.operator.Summarize;

/**
 * Test for {@link SummarizeOperatorGenerator}.
 */
public class SummarizeOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * test for mapper.
     */
    @Test
    public void mapper() {
        UserOperator operator = load("simple").build();
        AggregateNodeInfo info = (AggregateNodeInfo) generate(operator);
        assertThat(info.getMapperType(), is(notNullValue()));
        loading(info.getMapperType(), c -> {
            @SuppressWarnings("unchecked")
            Function<Object, Object> f = (Function<Object, Object>) c.newInstance();
            MockDataModel m = new MockDataModel(100, new BigDecimal("123"), "Hello, world!");
            MockSummarized s = (MockSummarized) f.apply(m);

            assertThat(s.getKeyOption(), is(new IntOption(100)));
            assertThat(s.getCntOption(), is(new LongOption(1)));
            assertThat(s.getMinOption(), is(new StringOption("Hello, world!")));
            assertThat(s.getMaxOption(), is(new StringOption("Hello, world!")));
            assertThat(s.getSumOption(), is(new DecimalOption(new BigDecimal("123"))));
        });
    }

    /**
     * test for combiner.
     */
    @Test
    public void copier() {
        UserOperator operator = load("simple").build();
        AggregateNodeInfo info = (AggregateNodeInfo) generate(operator);
        loading(info.getCopierType(), c -> {
            @SuppressWarnings("unchecked")
            ObjectCopier<MockSummarized> obj = (ObjectCopier<MockSummarized>) c.newInstance();
            MockSummarized orig = new MockSummarized(1, "3.14", "v");
            MockSummarized copy = obj.newCopy(orig);
            assertThat(copy.getKeyOption(), is(orig.getKeyOption()));
            assertThat(copy.getSumOption(), is(orig.getSumOption()));
            assertThat(copy.getMinOption(), is(orig.getMinOption()));
            assertThat(copy, is(not(sameInstance(orig))));
        });
    }

    /**
     * test for combiner.
     */
    @Test
    public void combiner() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockSummarized> results = new MockSink<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(results);
            r.add(cogroup(new Object[][] {
                {
                    new MockSummarized(1, "10", "A"),
                    new MockSummarized(1, "20", "B"),
                    new MockSummarized(1, "30", "C"),
                }
            }));
        });
        assertThat(results.get(e -> e.getKeyOption().get()), contains(1));
        assertThat(results.get(e -> e.getCntOption().get()), contains(3L));
        assertThat(results.get(e -> e.getMinOption().getAsString()), contains("A"));
        assertThat(results.get(e -> e.getMaxOption().getAsString()), contains("C"));
        assertThat(results.get(e -> e.getSumOption().get()), contains(new BigDecimal("60")));
    }

    /**
     * cache - identical.
     */
    @Test
    public void cache() {
        UserOperator operator = load("simple").build();
        NodeInfo a = generate(operator);
        NodeInfo b = generate(operator);
        assertThat(b, useCacheOf(a));
    }

    /**
     * cache - different methods.
     */
    @Test
    public void cache_diff_method() {
        UserOperator opA = load("simple").build();
        UserOperator opB = load("renamed").build();
        NodeInfo a = generate(opA);
        NodeInfo b = generate(opB);
        assertThat(b, not(useCacheOf(a)));
    }

    private Builder load(String name) {
        return OperatorExtractor.extract(Summarize.class, Op.class, name)
                .input("in", Descriptions.typeOf(MockDataModel.class), Groups.parse(Arrays.asList("key")))
                .output("out", Descriptions.typeOf(MockSummarized.class));
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @Summarize
        public MockSummarized simple(MockDataModel in) {
            throw new AssertionError();
        }

        @Summarize
        public MockSummarized renamed(MockDataModel in) {
            return simple(in);
        }
    }

    @SuppressWarnings("javadoc")
    @Summarized(term = @Term(
            source = MockDataModel.class,
            foldings = {
                    @Folding(source = "key", destination = "key", aggregator = Aggregator.ANY),
                    @Folding(source = "key", destination = "cnt", aggregator = Aggregator.COUNT),
                    @Folding(source = "value", destination = "min", aggregator = Aggregator.MIN),
                    @Folding(source = "value", destination = "max", aggregator = Aggregator.MAX),
                    @Folding(source = "sort", destination = "sum", aggregator = Aggregator.SUM)
            },
            shuffle = @Key(group = "key")
            ))
    public static class MockSummarized implements DataModel<MockSummarized> {

        private final IntOption keyOption = new IntOption();

        private final LongOption cntOption = new LongOption();

        private final StringOption minOption = new StringOption();

        private final StringOption maxOption = new StringOption();

        private final DecimalOption sumOption = new DecimalOption();

        public MockSummarized() {
            return;
        }

        @SuppressWarnings("deprecation")
        public MockSummarized(int k, String sort, String value) {
            keyOption.modify(k);
            cntOption.modify(1L);
            minOption.modify(value);
            maxOption.modify(value);
            sumOption.modify(new BigDecimal(sort));
        }

        public IntOption getKeyOption() {
            return keyOption;
        }

        public LongOption getCntOption() {
            return cntOption;
        }

        public StringOption getMinOption() {
            return minOption;
        }

        public StringOption getMaxOption() {
            return maxOption;
        }

        public DecimalOption getSumOption() {
            return sumOption;
        }

        @SuppressWarnings("deprecation")
        @Override
        public void reset() {
            keyOption.setNull();
            cntOption.setNull();
            minOption.setNull();
            maxOption.setNull();
            sumOption.setNull();
        }

        @SuppressWarnings("deprecation")
        @Override
        public void copyFrom(MockSummarized other) {
            keyOption.copyFrom(other.keyOption);
            cntOption.copyFrom(other.cntOption);
            minOption.copyFrom(other.minOption);
            maxOption.copyFrom(other.maxOption);
            sumOption.copyFrom(other.sumOption);
        }
    }
}
