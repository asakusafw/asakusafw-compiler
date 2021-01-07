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
package com.asakusafw.lang.compiler.model.graph;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;

/**
 * Test for {@link FlowOperator}.
 */
public class FlowOperatorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockOperators operators = new MockOperators()
            .input("in")
            .operator("body", "in", "out")
            .output("out")
            .connect("in", "body")
            .connect("body", "out");
        OperatorGraph graph = operators.toGraph();

        FlowOperator operator = FlowOperator.builder(classOf(FlowOperatorTest.class), graph)
                .input("in", typeOf(String.class))
                .output("out", typeOf(Integer.class))
                .argument("type", valueOf(Integer.class))
                .build();
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.FLOW));
        assertThat(operator.getProperties(), hasSize(3));
        assertThat(operator.findInput("in").getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.findInput("in").getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.findOutput("out").getDataType(), is((Object) typeOf(Integer.class)));
        assertThat(operator.findOutput("out").getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.findArgument("type").getValue(), is(valueOf(Integer.class)));
        assertThat(operator.getOperatorGraph(), is(graph));
    }

    /**
     * test for copy.
     */
    @Test
    public void copy() {
        MockOperators operators = new MockOperators()
            .input("in")
            .operator("body", "in", "out")
            .output("out")
            .connect("in", "body")
            .connect("body", "out");
        OperatorGraph graph = operators.toGraph();

        FlowOperator operator = FlowOperator.builder(classOf(FlowOperatorTest.class), graph)
                .input("in", typeOf(String.class))
                .output("out", typeOf(Integer.class))
                .argument("type", valueOf(Integer.class))
                .build();
        FlowOperator copy = operator.copy();
        assertThat(copy, is(not(sameInstance(operator))));
        assertThat(copy.toString(), operator.getOperatorKind(), is(OperatorKind.FLOW));
        assertThat(copy.getProperties(), hasSize(3));
        assertThat(copy.findInput("in").getDataType(), is((Object) typeOf(String.class)));
        assertThat(copy.findInput("in").getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.findOutput("out").getDataType(), is((Object) typeOf(Integer.class)));
        assertThat(copy.findOutput("out").getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.findArgument("type").getValue(), is(valueOf(Integer.class)));
        assertThat(copy.getOperatorGraph(), is(not(sameInstance(graph))));

        new MockOperators(copy.getOperatorGraph().rebuild().getOperators())
            .assertOperator("in", OperatorKind.INPUT)
            .assertOperator("body", OperatorKind.USER)
            .assertOperator("out", OperatorKind.OUTPUT)
            .assertConnected("in", "body")
            .assertConnected("body", "out");
    }
}
