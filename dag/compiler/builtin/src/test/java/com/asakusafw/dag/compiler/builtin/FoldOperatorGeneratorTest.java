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

import java.util.Arrays;

import org.junit.Test;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.AggregateNodeInfo;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.runtime.adapter.ObjectCombiner;
import com.asakusafw.dag.runtime.adapter.ObjectCopier;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockSink;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Groups;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.operator.Fold;

/**
 * Test for {@link FoldOperatorGenerator}.
 */
public class FoldOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        UserOperator operator = load("simple")
                .build();
        AggregateNodeInfo info = (AggregateNodeInfo) generate(operator);
        MockSink<MockDataModel> results = new MockSink<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(results);
            r.add(cogroup(new Object[][] {
                {
                    new MockDataModel(1, "A"),
                    new MockDataModel(2, "A"),
                }
            }));
        });
        assertThat(results.get(e -> e.getKey()), contains(3));
        assertThat(results.get(e -> e.getValue()), contains("!"));

        loading(info.getCopierType(), c -> {
            @SuppressWarnings("unchecked")
            ObjectCopier<MockDataModel> obj = (ObjectCopier<MockDataModel>) c.newInstance();
            MockDataModel orig = new MockDataModel(1, "A");
            MockDataModel copy = obj.newCopy(orig);
            assertThat(copy.getKey(), is(orig.getKey()));
            assertThat(copy.getValue(), is(orig.getValue()));
            assertThat(copy, is(not(sameInstance(orig))));
        });
        loading(info.getCombinerType(), c -> {
            @SuppressWarnings("unchecked")
            ObjectCombiner<MockDataModel> obj = (ObjectCombiner<MockDataModel>) c.newInstance();
            MockDataModel a = new MockDataModel(1, "A");
            MockDataModel b = new MockDataModel(2, "B");
            obj.combine(a, b);
            assertThat(a.getKey(), is(3));
            assertThat(b.getKey(), is(2));
            assertThat(a.getValue(), is("!"));
            assertThat(b.getValue(), is("B"));
        });
    }

    /**
     * parameterized.
     */
    @Test
    public void parameterized() {
        UserOperator operator = load("parameterized")
                .argument("a", Descriptions.valueOf("?"))
                .build();
        AggregateNodeInfo info = (AggregateNodeInfo) generate(operator);
        MockSink<MockDataModel> results = new MockSink<>();
        loading(info, c -> {
            // fold operator embeds its arguments
            Result<Object> r = c.newInstance(results);
            r.add(cogroup(new Object[][] {
                {
                    new MockDataModel(1, "A"),
                    new MockDataModel(2, "A"),
                }
            }));
        });
        assertThat(results.get(e -> e.getKey()), contains(3));
        assertThat(results.get(e -> e.getValue()), contains("?"));

        loading(info.getCopierType(), c -> {
            @SuppressWarnings("unchecked")
            ObjectCopier<MockDataModel> obj = (ObjectCopier<MockDataModel>) c.newInstance();
            MockDataModel orig = new MockDataModel(1, "A");
            MockDataModel copy = obj.newCopy(orig);
            assertThat(copy.getKey(), is(orig.getKey()));
            assertThat(copy.getValue(), is(orig.getValue()));
            assertThat(copy, is(not(sameInstance(orig))));
        });
        loading(info.getCombinerType(), c -> {
            @SuppressWarnings("unchecked")
            ObjectCombiner<MockDataModel> obj = (ObjectCombiner<MockDataModel>) c.newInstance();
            MockDataModel a = new MockDataModel(1, "A");
            MockDataModel b = new MockDataModel(2, "B");
            obj.combine(a, b);
            assertThat(a.getKey(), is(3));
            assertThat(b.getKey(), is(2));
            assertThat(a.getValue(), is("?"));
            assertThat(b.getValue(), is("B"));
        });
    }

    /**
     * cache - simple case.
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

    /**
     * cache - different arguments.
     */
    @Test
    public void cache_diff_argument() {
        UserOperator opA = load("parameterized")
                .argument("parameterized", Descriptions.valueOf("a"))
                .build();
        UserOperator opB = load("parameterized")
                .argument("parameterized", Descriptions.valueOf("b"))
                .build();
        NodeInfo a = generate(opA);
        NodeInfo b = generate(opB);
        assertThat("fold operator embeds its arguments", b, not(useCacheOf(a)));
    }

    private Builder load(String name) {
        return OperatorExtractor.extract(Fold.class, Op.class, name)
                .input("in", Descriptions.typeOf(MockDataModel.class), Groups.parse(Arrays.asList("key")))
                .output("out", Descriptions.typeOf(MockDataModel.class));
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @Fold
        public void simple(MockDataModel a, MockDataModel b) {
            parameterized(a, b, "!");
        }

        @Fold
        public void renamed(MockDataModel a, MockDataModel b) {
            simple(a, b);
        }

        @Fold
        public void parameterized(MockDataModel a, MockDataModel b, String value) {
            a.setKey(a.getKey() + b.getKey());
            a.setValue(value);
        }
    }
}
