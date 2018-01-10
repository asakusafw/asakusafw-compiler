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
package com.asakusafw.lang.compiler.analyzer.util;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.vocabulary.operator.MasterBranch;
import com.asakusafw.vocabulary.operator.MasterCheck;
import com.asakusafw.vocabulary.operator.MasterJoin;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;
import com.asakusafw.vocabulary.operator.MasterSelection;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Test for {@link MasterJoinOperatorUtil}.
 */
public class MasterJoinOperatorUtilTest {

    private final ClassLoader cl = getClass().getClassLoader();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        UserOperator operator = extract(MasterJoin.class, Simple.class, "m0");
        assertThat(MasterJoinOperatorUtil.isSupported(operator), is(true));
        assertThat(MasterJoinOperatorUtil.getSelection(cl, operator), is(nullValue()));
    }

    /**
     * check supports.
     * @throws Exception if failed
     */
    @Test
    public void support() throws Exception {
        UserOperator m0 = extract(MasterJoin.class, Support.class, "m0");
        UserOperator m1 = extract(MasterCheck.class, Support.class, "m1");
        UserOperator m2 = extract(MasterBranch.class, Support.class, "m2");
        UserOperator m3 = extract(MasterJoinUpdate.class, Support.class, "m3");
        UserOperator m4 = extract(Update.class, Support.class, "m4");
        CoreOperator m5 = CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build();

        assertThat(MasterJoinOperatorUtil.isSupported(m0), is(true));
        assertThat(MasterJoinOperatorUtil.isSupported(m1), is(true));
        assertThat(MasterJoinOperatorUtil.isSupported(m2), is(true));
        assertThat(MasterJoinOperatorUtil.isSupported(m3), is(true));
        assertThat(MasterJoinOperatorUtil.isSupported(m4), is(false));
        assertThat(MasterJoinOperatorUtil.isSupported(m5), is(false));
    }

    /**
     * w/ selection.
     * @throws Exception if failed
     */
    @Test
    public void with_selection() throws Exception {
        UserOperator m0 = extract(MasterJoin.class, WithSelection.class, "m0");
        UserOperator m1 = extract(MasterJoin.class, WithSelection.class, "m1");
        UserOperator m2 = extract(MasterJoin.class, WithSelection.class, "m2");
        UserOperator m3 = extract(MasterJoin.class, WithSelection.class, "m3");

        Method s0 = MasterJoinOperatorUtil.getSelection(cl, m0);
        assertThat(s0, is(nullValue()));

        Method s1 = MasterJoinOperatorUtil.getSelection(cl, m1);
        assertThat(s1, is(notNullValue()));
        assertThat(s1.getDeclaringClass(), is((Object) WithSelection.class));
        assertThat(s1.getName(), is("s1"));

        try {
            MasterJoinOperatorUtil.getSelection(cl, m2);
            fail();
        } catch (ReflectiveOperationException e) {
            // ok.
        }

        try {
            MasterJoinOperatorUtil.getSelection(cl, m3);
            fail();
        } catch (ReflectiveOperationException e) {
            // ok.
        }
    }

    /**
     * unsupported operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_unsupported() throws Exception {
        MasterJoinOperatorUtil.getSelection(cl, CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build());
    }

    private UserOperator extract(
            Class<? extends Annotation> annotationType,
            Class<?> operatorClass,
            String methodName) {
        return OperatorExtractor.extract(annotationType, operatorClass, methodName)
                .input("mst", typeOf(String.class))
                .input("tx", typeOf(String.class))
                .output("joined", typeOf(String.class))
                .output("missed", typeOf(String.class))
                .build();
    }

    @SuppressWarnings("javadoc")
    public static abstract class Simple {

        @MasterJoin
        public abstract void m0();
    }

    @SuppressWarnings("javadoc")
    public static abstract class Support {

        @MasterJoin
        public abstract void m0();

        @MasterCheck
        public abstract void m1();

        @MasterBranch
        public abstract void m2();

        @MasterJoinUpdate
        public abstract void m3();

        @Update
        public abstract void m4();
    }

    @SuppressWarnings("javadoc")
    public static abstract class WithSelection {

        @MasterJoin()
        public abstract void m0();

        @MasterJoin(selection = "s1")
        public abstract void m1();

        @MasterJoin(selection = "s2")
        public abstract void m2();

        @MasterJoin(selection = "s3")
        public abstract void m3();

        @MasterSelection
        public abstract void s1();

        public abstract void s2();
    }
}
