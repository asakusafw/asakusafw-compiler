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

import org.junit.Test;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.dag.runtime.testing.MockValueModel;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.core.GroupView;
import com.asakusafw.runtime.testing.MockResult;
import com.asakusafw.vocabulary.operator.Extract;

/**
 * Test for {@link ExtractOperatorGenerator}.
 */
public class ExtractOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        UserOperator operator = load("simple")
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .build();
        NodeInfo info = generate(operator);
        MockResult<MockValueModel> results = new MockResult<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(results);
            r.add(new MockDataModel("Hello"));
        });
        assertThat(Lang.project(results.getResults(), e -> e.getValue()), contains("Hello!"));
    }

    /**
     * simple case.
     */
    @Test
    public void multiple() {
        UserOperator operator = load("multiout")
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
            r.add(new MockDataModel("Hello"));
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
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .argument("p", Descriptions.valueOf("-"))
                .build();
        NodeInfo info = generate(operator);
        MockResult<MockValueModel> results = new MockResult<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(results, "?");
            r.add(new MockDataModel("Hello"));
        });
        assertThat(Lang.project(results.getResults(), e -> e.getValue()), contains("Hello?"));
    }

    /**
     * w/ data tables.
     */
    @Test
    public void datatable() {
        UserOperator operator = load("table")
                .input("t0", Descriptions.typeOf(MockDataModel.class), c -> c
                        .group(group("key"))
                        .unit(InputUnit.WHOLE))
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .build();
        NodeInfo info = generate(operator);
        MockTable<MockDataModel> table = new MockTable<>(MockDataModel::getKeyOption)
                .add(new MockDataModel(1, "world"));
        MockResult<MockValueModel> results = new MockResult<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(table, results);
            r.add(new MockDataModel(1, "Hello"));
        });
        assertThat(Lang.project(results.getResults(), e -> e.getValue()), contains("Helloworld"));
    }

    /**
     * cache - identical.
     */
    @Test
    public void cache() {
        UserOperator operator = load("simple")
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .build();
        NodeInfo a = generate(operator);
        NodeInfo b = generate(operator);
        assertThat(b, useCacheOf(a));
    }

    /**
     * cache - different methods.
     */
    @Test
    public void cache_diff_method() {
        UserOperator opA = load("simple")
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .build();
        UserOperator opB = load("renamed")
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
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .argument("parameterized", Descriptions.valueOf("a"))
                .build();
        UserOperator opB = load("parameterized")
                .output("r0", Descriptions.typeOf(MockValueModel.class))
                .argument("parameterized", Descriptions.valueOf("b"))
                .build();
        NodeInfo a = generate(opA);
        NodeInfo b = generate(opB);
        assertThat(b, useCacheOf(a));
    }

    private Builder load(String name) {
        return OperatorExtractor.extract(Extract.class, Op.class, name)
                .input("in", Descriptions.typeOf(MockDataModel.class));
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @Extract
        public void simple(MockDataModel m, Result<MockValueModel> r0) {
            parameterized(m, r0, "!");
        }

        @Extract
        public void renamed(MockDataModel m, Result<MockValueModel> r0) {
            simple(m, r0);
        }

        @Extract
        public void multiout(MockDataModel m, Result<MockDataModel> r0, Result<MockKeyValueModel> r1, Result<MockValueModel> r2) {
            r0.add(new MockDataModel(m.getValue() + "0"));
            r1.add(new MockKeyValueModel(m.getValue() + "1"));
            r2.add(new MockValueModel(m.getValue() + "2"));
        }

        @Extract
        public void parameterized(MockDataModel m, Result<MockValueModel> r0, String parameter) {
            r0.add(new MockValueModel(m.getValue() + parameter));
        }

        @Extract
        public void table(MockDataModel m, GroupView<MockDataModel> t, Result<MockValueModel> r0) {
            parameterized(m, r0, t.find(m.getKeyOption()).get(0).getValue());
        }
    }
}
