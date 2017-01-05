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
package com.asakusafw.lang.compiler.model.graph;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalPort.PortKind;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;

/**
 * Test for {@link ExternalInput}.
 */
public class ExternalInputTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ExternalInputInfo info = new ExternalInputInfo.Basic(
                new ClassDescription("Dummy"),
                "testing",
                classOf(String.class),
                ExternalInputInfo.DataSize.SMALL);

        ExternalInput operator = ExternalInput.newInstance("in", info);
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.INPUT));
        assertThat(operator.getPortKind(), is(PortKind.INPUT));
        assertThat(operator.getName(), is("in"));
        assertThat(operator.getOperatorPort().getName(), is(ExternalInput.PORT_NAME));
        assertThat(operator.getOperatorPort().getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getInfo(), is(info));
    }

    /**
     * test for copy.
     */
    @Test
    public void copy() {
        ExternalInput operator = ExternalInput.newInstance("in", typeOf(String.class));
        ExternalInput copy = operator.copy();
        assertThat(copy.toString(), copy.getOperatorKind(), is(OperatorKind.INPUT));
        assertThat(copy.getPortKind(), is(PortKind.INPUT));
        assertThat(copy.getName(), is("in"));
        assertThat(copy.getOperatorPort().getName(), is(ExternalInput.PORT_NAME));
        assertThat(copy.getOperatorPort().getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.getDataType(), is((Object) typeOf(String.class)));
        assertThat(copy.getInfo(), is(nullValue()));
    }
}
