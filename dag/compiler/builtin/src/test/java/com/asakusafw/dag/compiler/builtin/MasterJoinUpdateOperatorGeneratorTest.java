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
import java.util.Objects;

import org.junit.Test;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.table.BasicDataTable;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.dag.runtime.testing.MockSink;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
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
        UserOperator operator = loadMerge("simple").build();
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
        UserOperator operator = loadMerge("simple").build();
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
        UserOperator operator = loadMerge("parameterized")
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
        UserOperator operator = loadMerge("selecting")
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
        UserOperator operator = loadMerge("fixed")
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
        UserOperator operator = loadTable("simple").build();
        NodeInfo info = generate(operator);
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
        UserOperator operator = loadTable("simple").build();
        NodeInfo info = generate(operator);
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
        UserOperator operator = loadTable("parameterized")
                .argument("p", Descriptions.valueOf("-"))
                .build();
        NodeInfo info = generate(operator);
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
        UserOperator operator = loadTable("selecting")
                .argument("p", Descriptions.valueOf("-"))
                .build();
        NodeInfo info = generate(operator);
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
        UserOperator operator = loadTable("fixed")
                .argument("p", Descriptions.valueOf("*"))
                .build();
        NodeInfo info = generate(operator);
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
     * w/ data tables.
     */
    @Test
    public void datatable() {
        UserOperator operator = loadMerge("table")
                .input("t0", Descriptions.typeOf(MockDataModel.class), c -> c
                        .group(group("key"))
                        .unit(InputUnit.WHOLE))
                .build();
        NodeInfo info = generate(operator);
        MockTable<MockDataModel> table = new MockTable<>(MockDataModel::getKeyOption)
                .add(new MockDataModel(0, "<TBL>"));
        MockSink<MockDataModel> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(table, j, m);
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
        assertThat(j.get(MockDataModel::getValue), containsInAnyOrder("M0T0<TBL>", "M0T1<TBL>"));
        assertThat(m.get(MockDataModel::getValue), hasSize(0));
    }

    /**
     * cache - identical.
     */
    @Test
    public void cache() {
        UserOperator operator = loadMerge("simple").build();
        NodeInfo a = generate(operator);
        NodeInfo b = generate(operator);
        assertThat(b, useCacheOf(a));
    }

    /**
     * cache - identical.
     */
    @Test
    public void cache_table() {
        UserOperator operator = loadTable("simple").build();
        NodeInfo a = generate(operator);
        NodeInfo b = generate(operator);
        assertThat(b, useCacheOf(a));
    }

    /**
     * cache - different methods.
     */
    @Test
    public void cache_diff_method() {
        UserOperator opA = loadMerge("simple").build();
        UserOperator opB = loadMerge("renamed").build();
        NodeInfo a = generate(opA);
        NodeInfo b = generate(opB);
        assertThat(b, not(useCacheOf(a)));
    }

    /**
     * cache - different arguments.
     */
    @Test
    public void cache_diff_argument() {
        UserOperator opA = loadMerge("parameterized")
                .argument("parameterized", Descriptions.valueOf("a"))
                .build();
        UserOperator opB = loadMerge("parameterized")
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
        UserOperator opA = loadMerge("simple").build();
        UserOperator opB = loadTable("simple").build();
        NodeInfo a = generate(opA);
        NodeInfo b = generate(opB);
        assertThat(b, not(useCacheOf(a)));
    }

    private KeyBuffer key(DataTable.Builder<?> builder, int key) {
        return builder.newKeyBuffer().append(new IntOption(key));
    }

    private Builder loadMerge(String name) {
        return OperatorExtractor.extract(MasterJoinUpdate.class, Op.class, name)
                .input("master", Descriptions.typeOf(MockKeyValueModel.class), c -> c
                        .group(group("key"))
                        .unit(InputUnit.GROUP))
                .input("transaction", Descriptions.typeOf(MockDataModel.class), c -> c
                        .group(group("key"))
                        .unit(InputUnit.GROUP))
                .output("found", Descriptions.typeOf(MockDataModel.class))
                .output("missing", Descriptions.typeOf(MockDataModel.class));
    }

    private Builder loadTable(String name) {
        return OperatorExtractor.extract(MasterJoinUpdate.class, Op.class, name)
                .input("master", Descriptions.typeOf(MockKeyValueModel.class), c -> c
                        .group(group("key"))
                        .unit(InputUnit.WHOLE))
                .input("transaction", Descriptions.typeOf(MockDataModel.class), c -> c
                        .group(group("key"))
                        .unit(InputUnit.RECORD))
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

        @MasterJoinUpdate
        public void table(MockKeyValueModel k, MockDataModel v,
                com.asakusafw.runtime.core.DataTable<MockDataModel> t) {
            parameterized(k, v, t.find(k.getKeyOption()).get(0).getValue());
        }
    }
}
