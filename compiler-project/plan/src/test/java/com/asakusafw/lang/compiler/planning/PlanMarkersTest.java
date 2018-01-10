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
package com.asakusafw.lang.compiler.planning;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.testing.MockOperators;

/**
 * Test for {@link PlanMarkers}.
 */
public class PlanMarkersTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        assertThat(
                PlanMarkers.get(PlanMarkers.newInstance(classOf(String.class), PlanMarker.CHECKPOINT)),
                is(PlanMarker.CHECKPOINT));
        assertThat(
                PlanMarkers.get(CoreOperator.builder(CoreOperatorKind.CHECKPOINT).build()),
                is(nullValue()));
    }

    /**
     * insert marker into input w/o any connections.
     */
    @Test
    public void insert_input() {
        MockOperators operators = new MockOperators();
        operators
            .operator("a", "in", "out")
            .operator("b", "in", "out")
            .connect("a.out", "b.in")
            .bless("m", PlanMarkers.insert(PlanMarker.BEGIN, operators.getInput("a.in")))
            .assertConnected("m", "a.in");
    }

    /**
     * insert marker into input w/ connections.
     */
    @Test
    public void insert_input_connected() {
        MockOperators operators = new MockOperators();
        operators
            .operator("a", "in", "out")
            .operator("b", "in", "out")
            .connect("a.out", "b.in")
            .bless("m", PlanMarkers.insert(PlanMarker.BEGIN, operators.getInput("b.in")))
            .assertConnected("m", "b.in")
            .assertConnected("a.out", "m")
            .assertConnected("a.out", "b.in", false);
    }

    /**
     * insert marker into output w/o any connections.
     */
    @Test
    public void insert_output() {
        MockOperators operators = new MockOperators();
        operators
            .operator("a", "in", "out")
            .operator("b", "in", "out")
            .connect("a.out", "b.in")
            .bless("m", PlanMarkers.insert(PlanMarker.END, operators.getOutput("b.out")))
            .assertConnected("b.out", "m");
    }

    /**
     * insert marker into output w/ connections.
     */
    @Test
    public void insert_output_connected() {
        MockOperators operators = new MockOperators();
        operators
            .operator("a", "in", "out")
            .operator("b", "in", "out")
            .connect("a.out", "b.in")
            .bless("m", PlanMarkers.insert(PlanMarker.END, operators.getOutput("a.out")))
            .assertConnected("a.out", "m")
            .assertConnected("m", "b.in")
            .assertConnected("a.out", "b.in", false);
    }

    /**
     * insert marker into input w/ connections.
     */
    @Test
    public void insert_connection() {
        MockOperators operators = new MockOperators();
        operators
            .operator("a0", "in", "out")
            .operator("a1", "in", "out")
            .operator("b0", "in", "out")
            .operator("b1", "in", "out")
            .connect("a0.out", "b0.in")
            .connect("a1.out", "b0.in")
            .connect("a0.out", "b1.in")
            .connect("a1.out", "b1.in")
            .bless("m", PlanMarkers.insert(
                    PlanMarker.CHECKPOINT, operators.getOutput("a0.out"), operators.getInput("b0.in")))
            .assertConnected("a0.out", "m")
            .assertConnected("m", "b0.in")
            .assertConnected("a1.out", "m", false)
            .assertConnected("m", "b1.in", false)
            .assertConnected("a1.out", "m", false)
            .assertConnected("a0.out", "b0.in", false)
            .assertConnected("a1.out", "b0.in")
            .assertConnected("a0.out", "b1.in")
            .assertConnected("a1.out", "b1.in");
    }
}
