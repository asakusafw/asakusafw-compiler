/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.optimizer.basic;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.util.Collection;
import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.optimizer.OperatorEstimator;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriters;
import com.asakusafw.lang.compiler.optimizer.OptimizerTestRoot;

/**
 * Test for {@link CompositeOperatorRewriter}.
 */
public class CompositeOperatorRewriterTest extends OptimizerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        OperatorRewriter engine = CompositeOperatorRewriter.builder()
            .add(new MockRewriter())
            .build();

        MockOperators mock = new MockOperators()
            .marker("begin")
            .operator("a").connect("begin", "a")
            .marker("end").connect("a", "end");

        OperatorGraph graph = mock.toGraph();
        OperatorRewriters.apply(context(), OperatorEstimator.NULL, engine, graph);
        graph.rebuild();

        Collection<Operator> operators = graph.getOperators();
        assertThat(operators, containsInAnyOrder(mock.get("a")));
    }

    /**
     * empty.
     */
    @Test
    public void empty_elements() {
        OperatorRewriter engine = CompositeOperatorRewriter.builder().build();
        assertThat(engine, is(OperatorRewriter.NULL));
    }

    /**
     * multiple elements.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void multiple_elements() {
        OperatorRewriter engine = CompositeOperatorRewriter.builder()
                .add(new MockRewriter(), new MockRewriter())
                .build();
        assertThat(engine, is(instanceOf(CompositeOperatorRewriter.class)));

        List<OperatorRewriter> elements = ((CompositeOperatorRewriter) engine).getElements();
        assertThat(elements, contains(instanceOf(MockRewriter.class), instanceOf(MockRewriter.class)));
    }

    @SuppressWarnings("javadoc")
    public static class MockRewriter implements OperatorRewriter {
        @Override
        public void perform(Context context, OperatorGraph graph) {
            for (Operator operator : graph.getOperators()) {
                if (operator.getOperatorKind() == OperatorKind.MARKER) {
                    Operators.remove(operator);
                    graph.remove(operator);
                }
            }
        }
    }
}
