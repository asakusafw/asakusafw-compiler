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
package com.asakusafw.lang.compiler.model.graph;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;

/**
 * Test for {@link UserOperator}.
 */
public class UserOperatorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        UserOperator operator = UserOperator.builder(
                new AnnotationDescription(
                        classOf(SuppressWarnings.class),
                        Descriptions.valueOf(new String[] { "all" })),
                new MethodDescription(classOf(Mock.class), "method", typeOf(int.class)),
                classOf(MockImpl.class))
                .input("model", classOf(String.class))
                .output("out", classOf(Integer.class))
                .argument("value", valueOf(100))
                .build();
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.USER));
        assertThat(operator.getAnnotation().getDeclaringClass(), is(classOf(SuppressWarnings.class)));
        assertThat(operator.getMethod().getDeclaringClass(), is(classOf(Mock.class)));
        assertThat(operator.getMethod().getName(), is("method"));
        assertThat(operator.getImplementationClass(), is(classOf(MockImpl.class)));
        assertThat(operator.getProperties(), hasSize(3));
        assertThat(operator.findInput("model").getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.findInput("model").getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.findOutput("out").getDataType(), is((Object) typeOf(Integer.class)));
        assertThat(operator.findOutput("out").getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.findArgument("value").getValue(), is(valueOf(100)));
    }
    /**
     * test for copy.
     */
    @Test
    public void copy() {
        UserOperator operator = UserOperator.builder(
                new AnnotationDescription(
                        classOf(SuppressWarnings.class),
                        Descriptions.valueOf(new String[] { "all" })),
                new MethodDescription(classOf(Mock.class), "method", typeOf(int.class)),
                classOf(MockImpl.class))
                .input("model", classOf(String.class))
                .output("out", classOf(Integer.class))
                .argument("value", valueOf(100))
                .build();
        UserOperator copy = operator.copy();
        assertThat(copy, is(not(sameInstance(operator))));
        assertThat(copy.getAnnotation().getDeclaringClass(), is(classOf(SuppressWarnings.class)));
        assertThat(copy.getMethod().getDeclaringClass(), is(classOf(Mock.class)));
        assertThat(copy.getMethod().getName(), is("method"));
        assertThat(copy.getImplementationClass(), is(classOf(MockImpl.class)));
        assertThat(copy.getProperties(), hasSize(3));
        assertThat(copy.findInput("model").getDataType(), is((Object) typeOf(String.class)));
        assertThat(copy.findInput("model").getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.findOutput("out").getDataType(), is((Object) typeOf(Integer.class)));
        assertThat(copy.findOutput("out").getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.findArgument("value").getValue(), is(valueOf(100)));
    }

    abstract static class Mock {

        Integer method(String model, int value) {
            return Integer.valueOf(model);
        }
    }

    static class MockImpl extends Mock {
        // no members
    }
}
