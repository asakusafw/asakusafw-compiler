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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.DataTable;
import com.asakusafw.runtime.core.Report;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.core.legacy.LegacyReport;
import com.asakusafw.runtime.testing.MockResult;
import com.asakusafw.vocabulary.operator.Logging;

/**
 * Test for {@link UpdateOperatorGenerator}.
 */
public class LoggingOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * Report API initializer.
     */
    @Rule
    public final ExternalResource REPORT = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            LegacyReport.setDelegate(new Report.Default());
        }
        @Override
        protected void after() {
            LegacyReport.setDelegate(null);
        }
    };

    /**
     * simple case.
     */
    @Test
    public void simple() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockResult<MockDataModel> results = new MockResult<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(results);
            r.add(new MockDataModel("Hello"));
        });
        assertThat(Lang.project(results.getResults(), e -> e.getValue()), contains("Hello"));
    }

    /**
     * parameterized.
     */
    @Test
    public void parameterized() {
        UserOperator operator = load("parameterized").argument("p", Descriptions.valueOf("-")).build();
        NodeInfo info = generate(operator);
        MockResult<MockDataModel> results = new MockResult<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(results, "?");
            r.add(new MockDataModel("Hello"));
        });
        assertThat(Lang.project(results.getResults(), e -> e.getValue()), contains("Hello"));
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
                .build();
        NodeInfo info = generate(operator);
        MockTable<MockDataModel> table = new MockTable<>(MockDataModel::getKeyOption)
                .add(new MockDataModel(1, "world"));
        MockResult<MockDataModel> results = new MockResult<>();
        loading(info, c -> {
            Result<Object> r = c.newInstance(table, results);
            r.add(new MockDataModel(1, "Hello"));
        });
        assertThat(Lang.project(results.getResults(), e -> e.getValue()), contains("Hello"));
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

    private Builder load(String name) {
        return OperatorExtractor.extract(Logging.class, Op.class, name)
                .input("in", Descriptions.typeOf(MockDataModel.class))
                .output("out", Descriptions.typeOf(MockDataModel.class));
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @Logging
        public String simple(MockDataModel m) {
            return parameterized(m, "!");
        }

        @Logging
        public String renamed(MockDataModel m) {
            return simple(m);
        }

        @Logging
        public String parameterized(MockDataModel m, String parameter) {
            return m.getValue() + parameter;
        }

        @Logging
        public String table(MockDataModel m, DataTable<MockDataModel> t) {
            return parameterized(m, t.find(m.getKeyOption()).get(0).getValue());
        }
    }
}
