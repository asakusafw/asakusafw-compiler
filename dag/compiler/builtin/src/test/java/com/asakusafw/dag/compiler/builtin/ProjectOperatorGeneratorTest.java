/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
import com.asakusafw.dag.runtime.testing.MockKeyModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.dag.runtime.testing.MockValueModel;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.testing.MockResult;

/**
 * Test for {@link ProjectOperatorGenerator}.
 */
public class ProjectOperatorGeneratorTest extends OperatorNodeGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        CoreOperator operator = CoreOperator.builder(CoreOperatorKind.PROJECT)
            .input("in", Descriptions.typeOf(MockDataModel.class))
            .output("out", Descriptions.typeOf(MockValueModel.class))
            .build();
        NodeInfo info = generate(operator);
        MockResult<MockValueModel> results = new MockResult<>();
        loading(info, ctor -> {
            Result<Object> r = ctor.newInstance(results);
            r.add(new MockDataModel(100, "Hello, world!"));
        });
        List<String> r = Lang.project(results.getResults(), m -> m.getValue());
        assertThat(r, contains("Hello, world!"));
    }

    /**
     * cache - identical.
     */
    @Test
    public void cache() {
        CoreOperator opA = CoreOperator.builder(CoreOperatorKind.PROJECT)
                .input("in", Descriptions.typeOf(MockDataModel.class))
                .output("out", Descriptions.typeOf(MockValueModel.class))
                .build();
        NodeInfo a = generate(opA);
        NodeInfo b = generate(opA);
        assertThat(b, useCacheOf(a));
    }

    /**
     * cache - different I/O types.
     */
    @Test
    public void cache_diff_type() {
        CoreOperator opA = CoreOperator.builder(CoreOperatorKind.PROJECT)
                .input("in", Descriptions.typeOf(MockDataModel.class))
                .output("out", Descriptions.typeOf(MockValueModel.class))
                .build();
        CoreOperator opB = CoreOperator.builder(CoreOperatorKind.PROJECT)
                .input("in", Descriptions.typeOf(MockKeyValueModel.class))
                .output("out", Descriptions.typeOf(MockValueModel.class))
                .build();
        CoreOperator opC = CoreOperator.builder(CoreOperatorKind.PROJECT)
                .input("in", Descriptions.typeOf(MockDataModel.class))
                .output("out", Descriptions.typeOf(MockKeyModel.class))
                .build();
        NodeInfo a = generate(opA);
        NodeInfo b = generate(opB);
        NodeInfo c = generate(opC);
        assertThat(b, not(useCacheOf(a)));
        assertThat(c, not(useCacheOf(a)));
    }
}
