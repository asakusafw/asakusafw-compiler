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
package com.asakusafw.lang.compiler.extension.trace;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.extension.trace.testing.MockDataModel;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.trace.model.TraceSetting;
import com.asakusafw.trace.model.TraceSetting.Mode;
import com.asakusafw.trace.model.Tracepoint;
import com.asakusafw.trace.model.Tracepoint.PortKind;
import com.asakusafw.vocabulary.operator.Logging;
import com.asakusafw.vocabulary.operator.OperatorFactory;
import com.asakusafw.vocabulary.operator.OperatorInfo;
import com.asakusafw.vocabulary.operator.OperatorInfo.Input;
import com.asakusafw.vocabulary.operator.OperatorInfo.Output;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Test for {@link TraceOperatorWeaver}.
 */
public class TraceOperatorWeaverTest {

    /**
     * input - simple case.
     */
    @Test
    public void input() {
        OperatorGraph graph = graph();
        List<TraceSetting> settings = new ArrayList<>();
        settings.add(new TraceSetting(
                new Tracepoint(Ops.class.getName(), "update", PortKind.INPUT, "in"),
                Mode.IN_ORDER,
                Collections.emptyMap()));

        assertThat(graph.getOperators(), hasSize(1));

        TraceOperatorWeaver.perform(graph, getClass().getClassLoader(), settings);
        assertThat(graph.getOperators(), hasSize(2));

        Operator operator = find(graph, "update");
        assertThat(Operators.getPredecessors(operator), hasSize(1));
        assertThat(Operators.getSuccessors(operator), hasSize(0));

        Operator trace = Operators.getPredecessors(operator).iterator().next();
        checkTrace(trace);
    }

    /**
     * output - simple case.
     */
    @Test
    public void output() {
        OperatorGraph graph = graph();
        List<TraceSetting> settings = new ArrayList<>();
        settings.add(new TraceSetting(
                new Tracepoint(Ops.class.getName(), "update", PortKind.OUTPUT, "out"),
                Mode.IN_ORDER,
                Collections.emptyMap()));

        assertThat(graph.getOperators(), hasSize(1));

        TraceOperatorWeaver.perform(graph, getClass().getClassLoader(), settings);
        assertThat(graph.getOperators(), hasSize(2));

        Operator operator = find(graph, "update");
        assertThat(Operators.getPredecessors(operator), hasSize(0));
        assertThat(Operators.getSuccessors(operator), hasSize(1));

        Operator trace = Operators.getSuccessors(operator).iterator().next();
        checkTrace(trace);
    }

    private OperatorGraph graph() {
        UserOperator operator = OperatorExtractor.extract(Update.class, Ops.class, "update")
            .input("in", typeOf(MockDataModel.class))
            .output("out", typeOf(MockDataModel.class))
            .build();
        OperatorGraph results = new OperatorGraph();
        results.add(operator);
        return results;
    }

    private void checkTrace(Operator trace) {
        assertThat(trace.getOperatorKind(), is(OperatorKind.USER));
        UserOperator user = (UserOperator) trace;
        assertThat(user.getAnnotation().getDeclaringClass(), is(classOf(Logging.class)));
    }

    private Operator find(OperatorGraph graph, String name) {
        for (Operator operator : graph.getOperators()) {
            if (operator.getOperatorKind() != OperatorKind.USER) {
                continue;
            }
            UserOperator user = (UserOperator) operator;
            if (user.getMethod().getName().equals(name) == false) {
                continue;
            }
            return operator;
        }
        throw new AssertionError(name);
    }

    @SuppressWarnings("javadoc")
    public static class Ops {

        @Update
        public void update(MockDataModel data) {
            return;
        }
    }

    @SuppressWarnings("javadoc")
    @OperatorFactory(Ops.class)
    public static class OpsFactory {

        @OperatorInfo(
                kind = Update.class,
                input = {
                        @Input(position = 0, name = "in", type = MockDataModel.class)
                },
                output = {
                        @Output(name = "out", type = MockDataModel.class)
                },
                parameter = {})
        @Update
        public void update() {
            return;
        }
    }
}
