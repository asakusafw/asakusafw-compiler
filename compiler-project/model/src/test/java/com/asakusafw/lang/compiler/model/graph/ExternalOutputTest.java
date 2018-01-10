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
package com.asakusafw.lang.compiler.model.graph;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalPort.PortKind;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Test for {@link ExternalOutput}.
 */
public class ExternalOutputTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ExternalOutputInfo info = new ExternalOutputInfo.Basic(
                new ClassDescription("Dummy"),
                "testing",
                classOf(String.class));

        ExternalOutput operator = ExternalOutput.newInstance("out", info);
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.OUTPUT));
        assertThat(operator.getPortKind(), is(PortKind.OUTPUT));
        assertThat(operator.getName(), is("out"));
        assertThat(operator.getOperatorPort().getName(), is(ExternalOutput.PORT_NAME));
        assertThat(operator.getOperatorPort().getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getInfo(), is(info));
    }

    /**
     * w/ upstream.
     */
    @Test
    public void w_upstream() {
        ExternalInput upstream = ExternalInput.newInstance("in", typeOf(String.class));
        ExternalOutput operator = ExternalOutput.newInstance("out", upstream.getOperatorPort());
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.OUTPUT));
        assertThat(operator.getPortKind(), is(PortKind.OUTPUT));
        assertThat(operator.getName(), is("out"));
        assertThat(operator.getOperatorPort().getName(), is(ExternalOutput.PORT_NAME));
        assertThat(operator.getOperatorPort().getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.getOperatorPort().getOpposites(), hasItems(upstream.getOperatorPort()));
        assertThat(operator.getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getInfo(), is(nullValue()));
    }

    /**
     * test for copy.
     */
    @Test
    public void copy() {
        ExternalOutput operator = ExternalOutput.newInstance("out", typeOf(String.class));
        ExternalOutput copy = operator.copy();
        assertThat(copy.toString(), copy.getOperatorKind(), is(OperatorKind.OUTPUT));
        assertThat(copy.getPortKind(), is(PortKind.OUTPUT));
        assertThat(copy.getName(), is("out"));
        assertThat(copy.getOperatorPort().getName(), is(ExternalOutput.PORT_NAME));
        assertThat(copy.getOperatorPort().getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getInfo(), is(nullValue()));
    }
}
