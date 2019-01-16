/**
 * Copyright 2011-2019 Asakusa Framework Team.
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

import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;

/**
 * Test for {@link CoreOperator}.
 */
public class CoreOperatorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        CoreOperator operator = CoreOperator.builder(CoreOperatorKind.PROJECT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(Integer.class))
                .argument("type", valueOf(Integer.class))
                .build();
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.CORE));
        assertThat(operator.getCoreOperatorKind(), is(CoreOperatorKind.PROJECT));
        assertThat(operator.getProperties(), hasSize(3));
        assertThat(operator.findInput("in").getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.findInput("in").getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.findOutput("out").getDataType(), is((Object) typeOf(Integer.class)));
        assertThat(operator.findOutput("out").getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.findArgument("type").getValue(), is(valueOf(Integer.class)));

        assertThat(operator.getOriginalSerialNumber(), is(operator.getSerialNumber()));
    }

    /**
     * test for copy.
     */
    @Test
    public void copy() {
        CoreOperator operator = CoreOperator.builder(CoreOperatorKind.PROJECT)
                .input("in", classOf(String.class))
                .output("out", classOf(Integer.class))
                .argument("type", classOf(Integer.class))
                .build();
        CoreOperator copy = operator.copy();
        assertThat(copy, is(not(sameInstance(operator))));
        assertThat(copy.getCoreOperatorKind(), is(CoreOperatorKind.PROJECT));
        assertThat(copy.getProperties(), hasSize(3));
        assertThat(copy.findInput("in").getDataType(), is((Object) typeOf(String.class)));
        assertThat(copy.findInput("in").getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.findOutput("out").getDataType(), is((Object) typeOf(Integer.class)));
        assertThat(copy.findOutput("out").getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.findArgument("type").getValue(), is(valueOf(Integer.class)));

        assertThat(copy.getSerialNumber(), is(not(operator.getSerialNumber())));
        assertThat(copy.getOriginalSerialNumber(), is(operator.getOriginalSerialNumber()));
    }
}
