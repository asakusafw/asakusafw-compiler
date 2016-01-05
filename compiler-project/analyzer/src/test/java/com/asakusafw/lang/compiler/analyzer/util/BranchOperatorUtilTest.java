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
package com.asakusafw.lang.compiler.analyzer.util;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.vocabulary.operator.Branch;
import com.asakusafw.vocabulary.operator.MasterBranch;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Test for {@link BranchOperatorUtil}.
 */
public class BranchOperatorUtilTest {

    private final ClassLoader cl = getClass().getClassLoader();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        UserOperator m0 = extract(Branch.class, Ops.class, "m0");
        assertThat(BranchOperatorUtil.isSupported(m0), is(true));
        Map<Enum<?>, OperatorOutput> o0 = rev(BranchOperatorUtil.getOutputMap(cl, m0));
        assertThat(o0.keySet(), containsInAnyOrder((Enum<?>[]) State.values()));
        assertThat(o0.get(State.A).getName(), is("a"));
        assertThat(o0.get(State.B).getName(), is("b"));
        assertThat(o0.get(State.C).getName(), is("c"));
        assertThat(o0.get(State.VERY_LONG_NAME_CONSTANT_VALUE).getName(), is("veryLongNameConstantValue"));
    }

    /**
     * w/ number.
     * @throws Exception if failed
     */
    @Test
    public void numeric() throws Exception {
        UserOperator m = extract(Branch.class, Ops.class, "mN", WithNumber.values());
        assertThat(BranchOperatorUtil.isSupported(m), is(true));
        Map<Enum<?>, OperatorOutput> o0 = rev(BranchOperatorUtil.getOutputMap(cl, m));
        assertThat(o0.keySet(), containsInAnyOrder((Enum<?>[]) WithNumber.values()));
        assertThat(o0.get(WithNumber.CODE_100).getName(), is("code100"));
        assertThat(o0.get(WithNumber.CODE_200_X).getName(), is("code200X"));
        assertThat(o0.get(WithNumber.CODE_300_2).getName(), is("code3002"));
    }

    /**
     * check supports.
     * @throws Exception if failed
     */
    @Test
    public void support() throws Exception {
        UserOperator m0 = extract(Branch.class, Ops.class, "m0");
        UserOperator m1 = extract(MasterBranch.class, Ops.class, "m1");
        UserOperator m2 = extract(Update.class, Ops.class, "m2");
        CoreOperator m3 = CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build();

        assertThat(BranchOperatorUtil.isSupported(m0), is(true));
        assertThat(BranchOperatorUtil.isSupported(m1), is(true));
        assertThat(BranchOperatorUtil.isSupported(m2), is(false));
        assertThat(BranchOperatorUtil.isSupported(m3), is(false));
    }

    /**
     * unsupported operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_unsupported() throws Exception {
        BranchOperatorUtil.getOutputMap(cl, CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build());
    }

    /**
     * invalid operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_not_enum() throws Exception {
        BranchOperatorUtil.getOutputMap(cl, extract(Branch.class, Ops.class, "mX"));
    }

    /**
     * invalid operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_conflict() throws Exception {
        UserOperator operator = OperatorExtractor.extract(Branch.class, Ops.class, "m0")
                .input("in", typeOf(String.class))
                .output("a", typeOf(String.class))
                .output("b", typeOf(String.class))
                .output("c", typeOf(String.class))
                .output("veryLongNameConstantValue", typeOf(String.class))
                .output("C", typeOf(String.class)) // conflict
                .build();
        BranchOperatorUtil.getOutputMap(cl, operator);
    }

    /**
     * invalid operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_missing_constant() throws Exception {
        UserOperator operator = OperatorExtractor.extract(Branch.class, Ops.class, "m0")
                .input("in", typeOf(String.class))
                .output("a", typeOf(String.class))
                .output("b", typeOf(String.class))
                .output("c", typeOf(String.class))
                .output("d", typeOf(String.class))
                .output("veryLongNameConstantValue", typeOf(String.class))
                .build();
        BranchOperatorUtil.getOutputMap(cl, operator);
    }

    /**
     * invalid operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_missing_output() throws Exception {
        UserOperator operator = OperatorExtractor.extract(Branch.class, Ops.class, "m0")
                .input("in", typeOf(String.class))
                .output("a", typeOf(String.class))
                .output("b", typeOf(String.class))
                .output("veryLongNameConstantValue", typeOf(String.class))
                .build();
        BranchOperatorUtil.getOutputMap(cl, operator);
    }

    private UserOperator extract(
            Class<? extends Annotation> annotationType,
            Class<?> operatorClass,
            String methodName) {
        return extract(annotationType, operatorClass, methodName, State.values());
    }

    private UserOperator extract(
            Class<? extends Annotation> annotationType,
            Class<?> operatorClass,
            String methodName,
            Enum<?>[] constants) {
        UserOperator.Builder builder = OperatorExtractor.extract(annotationType, operatorClass, methodName)
                .input("in", typeOf(String.class));
        for (Enum<?> value : constants) {
            String name = PropertyName.of(value.name()).toMemberName();
            builder.output(name, typeOf(String.class));
        }
        return builder.build();
    }

    private Map<Enum<?>, OperatorOutput> rev(Map<OperatorOutput, Enum<?>> outputs) {
        Map<Enum<?>, OperatorOutput> results = new HashMap<>();
        for (Map.Entry<OperatorOutput, Enum<?>> entry : outputs.entrySet()) {
            assertThat(results.get(entry.getValue()), is(nullValue()));
            results.put(entry.getValue(), entry.getKey());
        }
        return results;
    }

    @SuppressWarnings("javadoc")
    public enum State {

        A,

        B,

        C,

        VERY_LONG_NAME_CONSTANT_VALUE,
    }

    @SuppressWarnings("javadoc")
    public enum WithNumber {

        CODE_100,

        CODE_200_X,

        CODE_300_2,
    }

    @SuppressWarnings("javadoc")
    public static abstract class Ops {

        @Branch
        public abstract State m0();

        @MasterBranch
        public abstract State m1();

        @Update
        public abstract State m2();

        @Branch
        public abstract WithNumber mN();

        @Branch
        public abstract void mX();
    }
}
