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

import java.lang.annotation.ElementType;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;

/**
 * Test for {@link MarkerOperator}.
 */
public class MarkerOperatorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MarkerOperator operator = MarkerOperator.builder(typeOf(String.class))
                .attribute(ElementType.class, ElementType.TYPE)
                .build();
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.MARKER));
        assertThat(operator.getProperties(), hasSize(2));
        assertThat(operator.getInput().getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getInput().getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.getOutput().getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getOutput().getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.getAttributeTypes(), hasSize(1));
        assertThat(operator.getAttribute(ElementType.class), is(ElementType.TYPE));
        assertThat(operator.getAttribute(Dummy.class), is(nullValue()));
    }

    /**
     * test for copy.
     */
    @Test
    public void copy() {
        MarkerOperator operator = MarkerOperator.builder(typeOf(String.class))
                .attribute(ElementType.class, ElementType.METHOD)
                .attribute(Dummy.class, new Dummy(1))
                .build();
        MarkerOperator copy = operator.copy();
        assertThat(copy.toString(), copy, is(not(sameInstance(operator))));
        assertThat(copy.getProperties(), hasSize(2));
        assertThat(copy.getInput().getDataType(), is((Object) typeOf(String.class)));
        assertThat(copy.getInput().getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.getOutput().getDataType(), is((Object) typeOf(String.class)));
        assertThat(copy.getOutput().getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.getAttributeTypes(), hasSize(2));
        assertThat(copy.getAttribute(ElementType.class), is(ElementType.METHOD));
        assertThat(copy.getAttribute(Dummy.class).value, is(2));
    }

    private static final class Dummy implements OperatorAttribute {

        final int value;

        public Dummy(int value) {
            this.value = value;
        }

        @Override
        public OperatorAttribute copy() {
            return new Dummy(value + 1);
        }
    }
}
