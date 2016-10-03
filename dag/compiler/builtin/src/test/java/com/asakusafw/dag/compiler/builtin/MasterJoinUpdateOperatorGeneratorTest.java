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
import java.util.List;
import java.util.Objects;

import org.junit.Test;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.compiler.model.graph.DataTableNode;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.table.BasicDataTable;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.dag.runtime.testing.MockSink;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Groups;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;
import com.asakusafw.vocabulary.operator.MasterSelection;

/**
 * Test for {@link MasterJoinUpdateOperatorGenerator}.
 */
public class MasterJoinUpdateOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple_merge() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(j, m);
            r.add(cogroup(new Object[][] {
                {
                    new MockKeyValueModel(0, "M0"),
                },
                {
                    new MockDataModel(0, "T0"),
                    new MockDataModel(0, "T1"),
                },
            }));
        });
        assertThat(j.get(MockDataModel::getValue), containsInAnyOrder("M0T0!", "M0T1!"));
        assertThat(m.get(MockDataModel::getValue), hasSize(0));
    }

    /**
     * missing masters.
     */
    @Test
    public void missing_merge() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(j, m);
            r.add(cogroup(new Object[][] {
                {
                },
                {
                    new MockDataModel(0, "T0"),
                    new MockDataModel(0, "T1"),
                },
            }));
        });
        assertThat(j.get(MockDataModel::getValue), hasSize(0));
        assertThat(m.get(MockDataModel::getValue), containsInAnyOrder("T0", "T1"));
    }

    /**
     * parameterized.
     */
    @Test
    public void parameterized_merge() {
        UserOperator operator = load("parameterized")
                .argument("p", Descriptions.valueOf("-"))
                .build();
        NodeInfo info = generate(operator);
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(j, m, "?");
            r.add(cogroup(new Object[][] {
                {
                    new MockKeyValueModel(0, "M0"),
                },
                {
                    new MockDataModel(0, "T0"),
                    new MockDataModel(0, "T1"),
                },
            }));
        });
        assertThat(j.get(MockDataModel::getValue), containsInAnyOrder("M0T0?", "M0T1?"));
        assertThat(m.get(MockDataModel::getValue), hasSize(0));
    }

    /**
     * w/ selector.
     */
    @Test
    public void selection_merge() {
        UserOperator operator = load("selecting")
                .argument("p", Descriptions.valueOf("-"))
                .build();
        NodeInfo info = generate(operator);
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(j, m, "M1");
            r.add(cogroup(new Object[][] {
                {
                    new MockKeyValueModel(0, "M0"),
                    new MockKeyValueModel(0, "M1"),
                    new MockKeyValueModel(0, "M2"),
                },
                {
                    new MockDataModel(0, "T0"),
                    new MockDataModel(0, "T1"),
                },
            }));
        });
        assertThat(j.get(MockDataModel::getValue), containsInAnyOrder("M1T0!", "M1T1!"));
        assertThat(m.get(MockDataModel::getValue), hasSize(0));
    }

    /**
     * w/ selector.
     */
    @Test
    public void selection_merge_fixed() {
        UserOperator operator = load("fixed")
                .argument("p", Descriptions.valueOf("*"))
                .build();
        NodeInfo info = generate(operator);
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(j, m, "*");
            r.add(cogroup(new Object[][] {
                {},
                {
                    new MockDataModel(0, "T0"),
                    new MockDataModel(0, "T1"),
                },
            }));
        });
        assertThat(j.get(MockDataModel::getValue), containsInAnyOrder("*T0*", "*T1*"));
        assertThat(m.get(MockDataModel::getValue), hasSize(0));
    }

    /**
     * simple case.
     */
    @Test
    public void simple_table() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generateWithTable(operator);
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            DataTable.Builder<MockKeyValueModel> t = new BasicDataTable.Builder<>();
            t.add(key(t, 0), new MockKeyValueModel(0, "M0"));
            Result<Object> r = ctor.newInstance(t.build(), j, m);
            r.add(new MockDataModel(0, "T0"));
            r.add(new MockDataModel(0, "T1"));
        });
        assertThat(j.get(MockDataModel::getValue), containsInAnyOrder("M0T0!", "M0T1!"));
        assertThat(m.get(MockDataModel::getValue), hasSize(0));
    }

    /**
     * missing masters on table.
     */
    @Test
    public void missing_table() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generateWithTable(operator);
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            DataTable.Builder<MockKeyValueModel> t = new BasicDataTable.Builder<>();
            Result<Object> r = ctor.newInstance(t.build(), j, m);
            r.add(new MockDataModel(0, "T0"));
            r.add(new MockDataModel(0, "T1"));
        });
        assertThat(j.get(MockDataModel::getValue), hasSize(0));
        assertThat(m.get(MockDataModel::getValue), containsInAnyOrder("T0", "T1"));
    }

    /**
     * parameterized.
     */
    @Test
    public void parameterized_table() {
        UserOperator operator = load("parameterized")
                .argument("p", Descriptions.valueOf("-"))
                .build();
        NodeInfo info = generateWithTable(operator);
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            DataTable.Builder<MockKeyValueModel> t = new BasicDataTable.Builder<>();
            t.add(key(t, 0), new MockKeyValueModel(0, "M0"));
            Result<Object> r = ctor.newInstance(t.build(), j, m, "?");
            r.add(new MockDataModel(0, "T0"));
            r.add(new MockDataModel(0, "T1"));
        });
        assertThat(j.get(MockDataModel::getValue), containsInAnyOrder("M0T0?", "M0T1?"));
        assertThat(m.get(MockDataModel::getValue), hasSize(0));
    }

    /**
     * w/ selector.
     */
    @Test
    public void selection_table() {
        UserOperator operator = load("selecting")
                .argument("p", Descriptions.valueOf("-"))
                .build();
        NodeInfo info = generateWithTable(operator);
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            DataTable.Builder<MockKeyValueModel> t = new BasicDataTable.Builder<>();
            t.add(key(t, 0), new MockKeyValueModel(0, "M0"));
            t.add(key(t, 0), new MockKeyValueModel(0, "M1"));
            t.add(key(t, 0), new MockKeyValueModel(0, "M2"));
            Result<Object> r = ctor.newInstance(t.build(), j, m, "M1");
            r.add(new MockDataModel(0, "T0"));
            r.add(new MockDataModel(0, "T1"));
        });
        assertThat(j.get(MockDataModel::getValue), containsInAnyOrder("M1T0!", "M1T1!"));
        assertThat(m.get(MockDataModel::getValue), hasSize(0));
    }

    /**
     * w/ selector.
     */
    @Test
    public void selection_table_fixed() {
        UserOperator operator = load("fixed")
                .argument("p", Descriptions.valueOf("*"))
                .build();
        NodeInfo info = generateWithTable(operator);
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(BasicDataTable.empty(), j, m, "*");
            r.add(new MockDataModel(0, "T0"));
            r.add(new MockDataModel(0, "T1"));
        });
        assertThat(j.get(MockDataModel::getValue), containsInAnyOrder("*T0*", "*T1*"));
        assertThat(m.get(MockDataModel::getValue), hasSize(0));
    }

    /**
     * orphaned master.
     */
    @Test
    public void orphaned_master() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator, c -> {
            // missing table, and is not a group input
            c.put(operator.findInput("master"), null);
            c.put(operator.findInput("transaction"), null);
            c.put(operator.findOutput("found"), result(Descriptions.typeOf(MockDataModel.class)));
            c.put(operator.findOutput("missing"), result(Descriptions.typeOf(MockDataModel.class)));
        });
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(j, m);
            r.add(new MockDataModel(0, "T0"));
            r.add(new MockDataModel(0, "T1"));
        });
        assertThat(j.get(MockDataModel::getValue), hasSize(0));
        assertThat(m.get(MockDataModel::getValue), containsInAnyOrder("T0", "T1"));
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
        assertThat(b, useCacheOf(a));
    }

    /**
     * cache - different strategy.
     */
    @Test
    public void cache_diff_strategy() {
        UserOperator opA = load("simple").build();
        NodeInfo a = generate(opA);
        NodeInfo b = generateWithTable(opA);
        assertThat(b, not(useCacheOf(a)));
    }

    private NodeInfo generateWithTable(UserOperator operator) {
        NodeInfo info = generate(operator, c -> {
            c.put(operator.findInput("master"), new DataTableNode("master",
                    Descriptions.typeOf(DataTable.class),
                    Descriptions.typeOf(MockKeyValueModel.class)));
            operator.getOutputs().forEach(o -> c.put(o, result(o.getDataType())));
        });
        return info;
    }

    private KeyBuffer key(DataTable.Builder<?> builder, int key) {
        return builder.newKeyBuffer().append(new IntOption(key));
    }

    private Builder load(String name) {
        return OperatorExtractor.extract(MasterJoinUpdate.class, Op.class, name)
                .input("master", Descriptions.typeOf(MockKeyValueModel.class), Groups.parse(Arrays.asList("key")))
                .input("transaction", Descriptions.typeOf(MockDataModel.class), Groups.parse(Arrays.asList("key")))
                .output("found", Descriptions.typeOf(MockDataModel.class))
                .output("missing", Descriptions.typeOf(MockDataModel.class));
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @MasterJoinUpdate
        public void simple(MockKeyValueModel k, MockDataModel v) {
            parameterized(k, v, "!");
        }

        @MasterJoinUpdate
        public void renamed(MockKeyValueModel k, MockDataModel v) {
            simple(k, v);
        }

        @MasterJoinUpdate
        public void parameterized(MockKeyValueModel k, MockDataModel v, String parameter) {
            v.setValue(k.getValue() + v.getValue() + parameter);
        }

        @MasterJoinUpdate(selection = "selector")
        public void selecting(MockKeyValueModel k, MockDataModel v, String parameter) {
            parameterized(k, v, "!");
        }

        @MasterSelection
        public MockKeyValueModel selector(List<MockKeyValueModel> k, MockDataModel v, String parameter) {
            return k.stream()
                    .filter(m -> Objects.equals(m.getValue(), parameter))
                    .findAny()
                    .orElse(null);
        }

        @MasterJoinUpdate(selection = "fixer")
        public void fixed(MockKeyValueModel k, MockDataModel v, String parameter) {
            parameterized(k, v, parameter);
        }

        @MasterSelection
        public MockKeyValueModel fixer(List<MockKeyValueModel> k, MockDataModel v, String parameter) {
            return new MockKeyValueModel(parameter);
        }
    }
}
