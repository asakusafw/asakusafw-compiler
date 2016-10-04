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

import com.asakusafw.dag.compiler.builtin.MasterJoinOperatorGeneratorTest.MockJoined;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.dag.runtime.testing.MockSink;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.graph.UserOperator.Builder;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.operator.Split;

/**
 * Test for {@link SplitOperatorGenerator}.
 */
public class SplitOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        UserOperator operator = load("simple").build();
        NodeInfo info = generate(operator);
        MockSink<MockKeyValueModel> m = new MockSink<>();
        MockSink<MockDataModel> t = new MockSink<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(m, t);
            r.add(new MockJoined(100, "M", "T"));
        });
        assertThat(m.get(MockKeyValueModel::getKey), contains(100));
        assertThat(m.get(MockKeyValueModel::getValue), contains("M"));
        assertThat(t.get(MockDataModel::getKey), contains(100));
        assertThat(t.get(MockDataModel::getValue), contains("T"));
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
        return OperatorExtractor.extract(Split.class, Op.class, name)
                .input("joined", Descriptions.typeOf(MockJoined.class))
                .output("mst", Descriptions.typeOf(MockKeyValueModel.class))
                .output("tx", Descriptions.typeOf(MockDataModel.class))
                ;
    }

    @SuppressWarnings("javadoc")
    public static class Op {

        @Split
        public void simple(MockJoined j, Result<MockKeyValueModel> m, Result<MockDataModel> t) {
            throw new AssertionError();
        }

        @Split
        public void renamed(MockJoined j, Result<MockKeyValueModel> m, Result<MockDataModel> t) {
            throw new AssertionError();
        }
    }
}
