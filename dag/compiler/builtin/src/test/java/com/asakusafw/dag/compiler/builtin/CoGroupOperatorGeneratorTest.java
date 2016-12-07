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

import java.util.List;

import org.junit.Test;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.dag.runtime.testing.MockSink;
import com.asakusafw.dag.runtime.testing.MockValueModel;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.testing.MockResult;
import com.asakusafw.vocabulary.attribute.BufferType;
import com.asakusafw.vocabulary.operator.CoGroup;

/**
 * Test for {@link CoGroupOperatorGenerator}.
 */
public class CoGroupOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        UserOperator operator = load("simple")
                .input("i0", Descriptions.typeOf(MockDataModel.class))
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .build();
        NodeInfo info = generate(operator);
        MockSink<MockValueModel> results = new MockSink<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(results);
            r.add(cogroup(new Object[][] {
                {
                    new MockDataModel("Hello"),
                }
            }));
        });
        assertThat(results.get(e -> e.getValue()), contains("Hello!"));
    }

    /**
     * multiple groups.
     */
    @Test
    public void multiple() {
        UserOperator operator = load("multiout")
                .input("i0", Descriptions.typeOf(MockDataModel.class))
                .output("r0", Descriptions.typeOf(MockDataModel.class))
                .output("r1", Descriptions.typeOf(MockKeyValueModel.class))
                .output("r2", Descriptions.typeOf(MockValueModel.class))
                .build();
        NodeInfo info = generate(operator);
        MockResult<MockDataModel> r0 = new MockResult<>();
        MockResult<MockKeyValueModel> r1 = new MockResult<>();
        MockResult<MockValueModel> r2 = new MockResult<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(r0, r1, r2);
            r.add(cogroup(new Object[][] {
                {
                    new MockDataModel("Hello"),
                }
            }));
        });
        assertThat(Lang.project(r0.getResults(), e -> e.getValue()), contains("Hello0"));
        assertThat(Lang.project(r1.getResults(), e -> e.getValue()), contains("Hello1"));
        assertThat(Lang.project(r2.getResults(), e -> e.getValue()), contains("Hello2"));
    }

    /**
     * parameterized.
     */
    @Test
    public void parameterized() {
        UserOperator operator = load("parameterized")
                .input("i0", Descriptions.typeOf(MockDataModel.class))
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .argument("p", Descriptions.valueOf("-"))
                .build();
        NodeInfo info = generate(operator);
        MockResult<MockValueModel> results = new MockResult<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(results, "?");
            r.add(cogroup(new Object[][] {
                {
                    new MockDataModel("Hello"),
                }
            }));
        });
        assertThat(Lang.project(results.getResults(), e -> e.getValue()), contains("Hello?"));
    }

    /**
     * w/ iterable.
     */
    @Test
    public void iterable() {
        UserOperator operator = load("iterable")
                .input("i0", Descriptions.typeOf(MockDataModel.class))
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .build();
        NodeInfo info = generate(operator);
        MockSink<MockValueModel> results = new MockSink<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(results);
            r.add(cogroup(new Object[][] {
                {
                    new MockDataModel("Hello"),
                }
            }));
        });
        assertThat(results.get(e -> e.getValue()), contains("Hello~"));
    }

    /**
     * w/ iterable + volatile.
     */
    @Test
    public void read_once() {
        UserOperator operator = load("iterable")
                .input("i0", Descriptions.typeOf(MockDataModel.class), c -> c.attribute(BufferType.VOLATILE))
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .build();
        NodeInfo info = generate(operator);
        MockSink<MockValueModel> results = new MockSink<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(results);
            r.add(cogroup(new Object[][] {
                {
                    new MockDataModel("Hello"),
                }
            }));
        });
        assertThat(results.get(e -> e.getValue()), contains("Hello~"));
    }

    /**
     * cache - simple case.
     */
    @Test
    public void cache() {
        UserOperator opA = load("simple")
                .input("i0", Descriptions.typeOf(MockDataModel.class))
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .build();
        NodeInfo a = generate(opA);
        NodeInfo b = generate(opA);
        assertThat(b, useCacheOf(a));
    }

    /**
     * cache - different methods.
     */
    @Test
    public void cache_diff_method() {
        UserOperator opA = load("simple")
                .input("i0", Descriptions.typeOf(MockDataModel.class))
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .build();
        UserOperator opB = load("renamed")
                .input("i0", Descriptions.typeOf(MockDataModel.class))
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .build();
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
                .input("i0", Descriptions.typeOf(MockDataModel.class))
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .argument("p", Descriptions.valueOf("a"))
                .build();
        UserOperator opB = load("parameterized")
                .input("i0", Descriptions.typeOf(MockDataModel.class))
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .argument("p", Descriptions.valueOf("b"))
                .build();
        NodeInfo a = generate(opA);
        NodeInfo b = generate(opB);
        assertThat(b, useCacheOf(a));
    }

    private static Builder load(String name) {
        return OperatorExtractor.extract(CoGroup.class, Op.class, name);
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @CoGroup
        public void simple(List<MockDataModel> i0, Result<MockValueModel> r0) {
            parameterized(i0, r0, "!");
        }

        @CoGroup
        public void iterable(Iterable<MockDataModel> i0, Result<MockValueModel> r0) {
            for (MockDataModel m : i0) {
                r0.add(new MockValueModel(m.getValue() + "~"));
            }
        }

        @CoGroup
        public void renamed(List<MockDataModel> i0, Result<MockValueModel> r0) {
            simple(i0, r0);
        }

        @CoGroup
        public void multiout(List<MockDataModel> i0,
                Result<MockDataModel> r0, Result<MockKeyValueModel> r1, Result<MockValueModel> r2) {
            for (MockDataModel m : i0) {
                r0.add(new MockDataModel(m.getValue() + "0"));
                r1.add(new MockKeyValueModel(m.getValue() + "1"));
                r2.add(new MockValueModel(m.getValue() + "2"));
            }
        }

        @CoGroup
        public void parameterized(List<MockDataModel> i0, Result<MockValueModel> r0, String parameter) {
            for (MockDataModel m : i0) {
                r0.add(new MockValueModel(m.getValue() + parameter));
            }
        }
    }
}
