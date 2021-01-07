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
package com.asakusafw.dag.compiler.builtin;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.table.BasicDataTable;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.dag.runtime.testing.MockSink;
import com.asakusafw.dag.runtime.testing.MockValueModel;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.core.View;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.vocabulary.model.Joined;
import com.asakusafw.vocabulary.model.Joined.Mapping;
import com.asakusafw.vocabulary.model.Joined.Term;
import com.asakusafw.vocabulary.model.Key;
import com.asakusafw.vocabulary.operator.MasterJoin;
import com.asakusafw.vocabulary.operator.MasterSelection;

/**
 * Test for {@link MasterJoinOperatorGenerator}.
 */
public class MasterJoinOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * simple case - merge join.
     */
    @Test
    public void simple_merge() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockJoined> j = new MockSink<>();
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
        assertThat(j.get(MockJoined::getKey), contains(0, 0));
        assertThat(j.get(MockJoined::getMaster), contains("M0", "M0"));
        assertThat(j.get(MockJoined::getTx), containsInAnyOrder("T0", "T1"));
        assertThat(m.get(), hasSize(0));
    }

    /**
     * missing.
     */
    @Test
    public void missing_merge() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockJoined> j = new MockSink<>();
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
        assertThat(j.get(), hasSize(0));
        assertThat(m.get(MockDataModel::getValue), containsInAnyOrder("T0", "T1"));
    }

    /**
     * w/ selector.
     */
    @Test
    public void selector_merge() {
        UserOperator operator = load("selecting").build();
        NodeInfo info = generate(operator);
        MockSink<MockJoined> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(j, m);
            r.add(cogroup(new Object[][] {
                {
                    new MockKeyValueModel(0, "M0"),
                    new MockKeyValueModel(0, "M1"),
                },
                {
                    new MockDataModel(0, "T0"),
                    new MockDataModel(0, "T1"),
                },
            }));
        });
        assertThat(j.get(MockJoined::getKey), contains(0, 0));
        assertThat(j.get(MockJoined::getMaster), contains("M1", "M1"));
        assertThat(j.get(MockJoined::getTx), containsInAnyOrder("T0", "T1"));
        assertThat(m.get(), hasSize(0));
    }

    /**
     * w/ extra parameters.
     */
    @Test
    public void parameterized_merge() {
        UserOperator operator = load("parameterized")
                .argument("s", Descriptions.valueOf("?"))
                .build();
        NodeInfo info = generate(operator);
        MockSink<MockJoined> j = new MockSink<>();
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
        assertThat(j.get(MockJoined::getKey), contains(0, 0));
        assertThat(j.get(MockJoined::getMaster), contains("M1", "M1"));
        assertThat(j.get(MockJoined::getTx), containsInAnyOrder("T0", "T1"));
        assertThat(m.get(), hasSize(0));
    }

    /**
     * w/ views.
     */
    @Test
    public void view_merge() {
        UserOperator operator = load("view")
                .input("view", Descriptions.typeOf(MockValueModel.class), c -> c
                        .unit(InputUnit.WHOLE)
                        .group(group()))
                .build();
        NodeInfo info = generate(operator);
        MockTable<MockValueModel> v = new MockTable<MockValueModel>()
                .add(new MockValueModel("M1"));
        MockSink<MockJoined> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(v, j, m);
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
        assertThat(j.get(MockJoined::getKey), contains(0, 0));
        assertThat(j.get(MockJoined::getMaster), contains("M1", "M1"));
        assertThat(j.get(MockJoined::getTx), containsInAnyOrder("T0", "T1"));
        assertThat(m.get(), hasSize(0));
    }

    /**
     * simple w/ table join.
     */
    @Test
    public void simple_table() {
        UserOperator operator = loadTable("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockJoined> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            DataTable.Builder<MockKeyValueModel> t = new BasicDataTable.Builder<>();
            t.add(key(t, 0), new MockKeyValueModel(0, "M0"));
            Result<Object> r = ctor.newInstance(t.build(), j, m);
            r.add(new MockDataModel(0, "T0"));
            r.add(new MockDataModel(0, "T1"));
        });
        assertThat(j.get(MockJoined::getKey), contains(0, 0));
        assertThat(j.get(MockJoined::getMaster), contains("M0", "M0"));
        assertThat(j.get(MockJoined::getTx), containsInAnyOrder("T0", "T1"));
        assertThat(m.get(), hasSize(0));
    }

    /**
     * missing.
     */
    @Test
    public void missing_table() {
        UserOperator operator = loadTable("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockJoined> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            DataTable.Builder<MockKeyValueModel> t = new BasicDataTable.Builder<>();
            Result<Object> r = ctor.newInstance(t.build(), j, m);
            r.add(new MockDataModel(0, "T0"));
            r.add(new MockDataModel(0, "T1"));
        });
        assertThat(j.get(), hasSize(0));
        assertThat(m.get(MockDataModel::getValue), containsInAnyOrder("T0", "T1"));
    }

    /**
     * w/ selector.
     */
    @Test
    public void selector_table() {
        UserOperator operator = loadTable("selecting").build();
        NodeInfo info = generate(operator);
        MockSink<MockJoined> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            DataTable.Builder<MockKeyValueModel> t = new BasicDataTable.Builder<>();
            t.add(key(t, 0), new MockKeyValueModel(0, "M0"));
            t.add(key(t, 0), new MockKeyValueModel(0, "M1"));
            Result<Object> r = ctor.newInstance(t.build(), j, m);
            r.add(new MockDataModel(0, "T0"));
            r.add(new MockDataModel(0, "T1"));
        });
        assertThat(j.get(MockJoined::getKey), contains(0, 0));
        assertThat(j.get(MockJoined::getMaster), contains("M1", "M1"));
        assertThat(j.get(MockJoined::getTx), containsInAnyOrder("T0", "T1"));
        assertThat(m.get(), hasSize(0));
    }

    /**
     * w/ parameters.
     */
    @Test
    public void parameterized_table() {
        UserOperator operator = loadTable("parameterized")
                .argument("s", Descriptions.valueOf("?"))
                .build();
        NodeInfo info = generate(operator);
        MockSink<MockJoined> j = new MockSink<>();
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
        assertThat(j.get(MockJoined::getKey), contains(0, 0));
        assertThat(j.get(MockJoined::getMaster), contains("M1", "M1"));
        assertThat(j.get(MockJoined::getTx), containsInAnyOrder("T0", "T1"));
        assertThat(m.get(), hasSize(0));
    }

    /**
     * w/ views.
     */
    @Test
    public void view_table() {
        UserOperator operator = loadTable("view")
                .input("view", Descriptions.typeOf(MockValueModel.class), c -> c
                        .unit(InputUnit.WHOLE)
                        .group(group()))
                .build();
        NodeInfo info = generate(operator);
        MockTable<MockValueModel> v = new MockTable<MockValueModel>()
                .add(new MockValueModel("M1"));
        MockSink<MockJoined> j = new MockSink<>();
        MockSink<MockDataModel> m = new MockSink<>();
        loading(info, ctor -> {
            DataTable.Builder<MockKeyValueModel> t = new BasicDataTable.Builder<>();
            t.add(key(t, 0), new MockKeyValueModel(0, "M0"));
            t.add(key(t, 0), new MockKeyValueModel(0, "M1"));
            t.add(key(t, 0), new MockKeyValueModel(0, "M2"));
            Result<Object> r = ctor.newInstance(t.build(), v, j, m);
            r.add(new MockDataModel(0, "T0"));
            r.add(new MockDataModel(0, "T1"));
        });
        assertThat(j.get(MockJoined::getKey), contains(0, 0));
        assertThat(j.get(MockJoined::getMaster), contains("M1", "M1"));
        assertThat(j.get(MockJoined::getTx), containsInAnyOrder("T0", "T1"));
        assertThat(m.get(), hasSize(0));
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
     * cache - different strategy.
     */
    @Test
    public void cache_diff_strategy() {
        UserOperator opA = load("simple").build();
        UserOperator opB = loadTable("simple").build();
        NodeInfo a = generate(opA);
        NodeInfo b = generate(opB);
        assertThat(b, not(useCacheOf(a)));
    }

    private KeyBuffer key(DataTable.Builder<?> builder, int key) {
        return builder.newKeyBuffer().append(new IntOption(key));
    }

    private Builder load(String name) {
        return OperatorExtractor.extract(MasterJoin.class, Op.class, name)
                .input("master", Descriptions.typeOf(MockKeyValueModel.class), c -> c
                        .group(group("key"))
                        .unit(InputUnit.GROUP))
                .input("transaction", Descriptions.typeOf(MockDataModel.class), c -> c
                        .group(group("key"))
                        .unit(InputUnit.GROUP))
                .output("joined", Descriptions.typeOf(MockJoined.class))
                .output("missed", Descriptions.typeOf(MockDataModel.class))
                ;
    }

    private Builder loadTable(String name) {
        return OperatorExtractor.extract(MasterJoin.class, Op.class, name)
                .input("master", Descriptions.typeOf(MockKeyValueModel.class), c -> c
                        .group(group("key"))
                        .unit(InputUnit.WHOLE))
                .input("transaction", Descriptions.typeOf(MockDataModel.class), c -> c
                        .group(group("key"))
                        .unit(InputUnit.RECORD))
                .output("joined", Descriptions.typeOf(MockJoined.class))
                .output("missed", Descriptions.typeOf(MockDataModel.class))
                ;
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @MasterJoin
        public MockJoined simple(MockKeyValueModel m, MockDataModel t) {
            throw new AssertionError();
        }

        @MasterJoin
        public MockJoined renamed(MockKeyValueModel m, MockDataModel t) {
            return simple(m, t);
        }

        @MasterJoin(selection = "selector")
        public MockJoined selecting(MockKeyValueModel m, MockDataModel t) {
            throw new AssertionError();
        }

        @MasterJoin(selection = "selector_parameterized")
        public MockJoined parameterized(MockKeyValueModel m, MockDataModel t, String s) {
            throw new AssertionError();
        }

        @MasterJoin(selection = "selector_view")
        public MockJoined view(MockKeyValueModel m, MockDataModel t, View<MockValueModel> view) {
            throw new AssertionError();
        }

        @MasterSelection
        public MockKeyValueModel selector(List<MockKeyValueModel> k, MockDataModel v) {
            return k.get(k.size() - 1);
        }

        @MasterSelection
        public MockKeyValueModel selector_parameterized(List<MockKeyValueModel> k, MockDataModel v, String s) {
            return k.stream()
                    .filter(m -> m.getValueOption().has(s))
                    .findFirst()
                    .orElse(null);
        }

        @MasterSelection
        public MockKeyValueModel selector_view(List<MockKeyValueModel> k, MockDataModel v, View<MockValueModel> view) {
            for (MockValueModel m : view) {
                return selector_parameterized(k, v, m.getValue());
            }
            return null;
        }
    }

    @Joined(terms = {
            @Term(source = MockKeyValueModel.class,
                    mappings = {
                            @Mapping(source = "key", destination = "key"),
                            @Mapping(source = "value", destination = "master")
                    },
                    shuffle = @Key(group = "key")),
            @Term(source = MockDataModel.class,
                    mappings = {
                            @Mapping(source = "key", destination = "key"),
                            @Mapping(source = "value", destination = "tx")
                    },
                    shuffle = @Key(group = "key"))
    })
    @SuppressWarnings({ "deprecation", "javadoc" })
    public static class MockJoined implements DataModel<MockJoined> {

        final IntOption keyOption = new IntOption();

        final StringOption masterOption = new StringOption();

        final StringOption txOption = new StringOption();

        public MockJoined() {
            return;
        }

        public MockJoined(int key, String master, String tx) {
            keyOption.modify(key);
            masterOption.modify(master);
            txOption.modify(tx);
        }

        public IntOption getKeyOption() {
            return keyOption;
        }

        public StringOption getMasterOption() {
            return masterOption;
        }

        public StringOption getTxOption() {
            return txOption;
        }

        public int getKey() {
            return keyOption.get();
        }

        public String getMaster() {
            return masterOption.getAsString();
        }

        public String getTx() {
            return txOption.getAsString();
        }

        @Override
        public void reset() {
            keyOption.setNull();
            masterOption.setNull();
            txOption.setNull();
        }

        @Override
        public void copyFrom(MockJoined other) {
            keyOption.copyFrom(other.keyOption);
            masterOption.copyFrom(other.masterOption);
            txOption.copyFrom(other.txOption);
        }
    }
}
