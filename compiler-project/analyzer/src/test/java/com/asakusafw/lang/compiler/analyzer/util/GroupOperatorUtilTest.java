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
package com.asakusafw.lang.compiler.analyzer.util;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.annotation.Annotation;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.vocabulary.attribute.BufferType;
import com.asakusafw.vocabulary.flow.processor.InputBuffer;
import com.asakusafw.vocabulary.operator.CoGroup;
import com.asakusafw.vocabulary.operator.GroupSort;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Test for {@link GroupOperatorUtil}.
 */
public class GroupOperatorUtilTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        UserOperator m0 = extract(GroupSort.class, Ops.class, "m0");
        assertThat(GroupOperatorUtil.isSupported(m0), is(true));
        assertThat(GroupOperatorUtil.getBufferType(m0.getInput(0)), is(BufferType.HEAP));
    }

    /**
     * check supports.
     * @throws Exception if failed
     */
    @Test
    public void support() throws Exception {
        UserOperator m0 = extract(GroupSort.class, Ops.class, "m0");
        UserOperator m1 = extract(CoGroup.class, Ops.class, "m1");
        UserOperator m2 = extract(GroupSort.class, Ops.class, "m2");
        UserOperator m3 = extract(CoGroup.class, Ops.class, "m3");
        UserOperator m4 = extract(Update.class, Ops.class, "m4");
        CoreOperator m5 = CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build();

        assertThat(GroupOperatorUtil.isSupported(m0), is(true));
        assertThat(GroupOperatorUtil.isSupported(m1), is(true));
        assertThat(GroupOperatorUtil.isSupported(m2), is(true));
        assertThat(GroupOperatorUtil.isSupported(m3), is(true));
        assertThat(GroupOperatorUtil.isSupported(m4), is(false));
        assertThat(GroupOperatorUtil.isSupported(m5), is(false));
    }

    /**
     * spill.
     */
    @Test
    public void spill() {
        UserOperator m0 = extract(GroupSort.class, Ops.class, "m0", BufferType.SPILL);
        assertThat(GroupOperatorUtil.isSupported(m0), is(true));
        assertThat(GroupOperatorUtil.getBufferType(m0.getInput(0)), is(BufferType.SPILL));
    }

    /**
     * once.
     */
    @Test
    public void once() {
        UserOperator m0 = extract(GroupSort.class, Ops.class, "m0", BufferType.VOLATILE);
        assertThat(GroupOperatorUtil.isSupported(m0), is(true));
        assertThat(GroupOperatorUtil.getBufferType(m0.getInput(0)), is(BufferType.VOLATILE));
    }

    /**
     * spill from operator annotation.
     */
    @Test
    public void inherit_spill() {
        UserOperator m0 = extract(GroupSort.class, Ops.class, "m2");
        assertThat(GroupOperatorUtil.isSupported(m0), is(true));
        assertThat(GroupOperatorUtil.getBufferType(m0.getInput(0)), is(BufferType.SPILL));
    }

    /**
     * unsupported operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_unsupported() throws Exception {
        CoreOperator operator = CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build();
        GroupOperatorUtil.getBufferType(operator.getInput(0));
    }

    private static UserOperator extract(
            Class<? extends Annotation> annotationType,
            Class<?> operatorClass,
            String methodName,
            Enum<?>... attributes) {
        return OperatorExtractor.extract(annotationType, operatorClass, methodName)
                .input("in", typeOf(String.class), c -> {
                    for (Enum<?> attr : attributes) {
                        c.attribute(attr);
                    }
                })
                .output("out", typeOf(String.class))
                .build();
    }

    @SuppressWarnings("javadoc")
    public static abstract class Ops {

        @GroupSort
        public abstract void m0();

        @CoGroup
        public abstract void m1();

        @GroupSort(inputBuffer = InputBuffer.ESCAPE)
        public abstract void m2();

        @CoGroup(inputBuffer = InputBuffer.ESCAPE)
        public abstract void m3();

        @Update
        public abstract void m4();
    }
}
