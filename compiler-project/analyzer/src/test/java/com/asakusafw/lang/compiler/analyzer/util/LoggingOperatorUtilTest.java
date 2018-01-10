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

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor;
import com.asakusafw.vocabulary.operator.Logging;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Test for {@link LoggingOperatorUtil}.
 */
public class LoggingOperatorUtilTest {

    private final ClassLoader cl = getClass().getClassLoader();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        UserOperator m0 = extract(Logging.class, Ops.class, "m0");
        assertThat(LoggingOperatorUtil.isSupported(m0), is(true));
        assertThat(LoggingOperatorUtil.getLogLevel(cl, m0), is(Logging.Level.ERROR));
    }

    /**
     * check supports.
     * @throws Exception if failed
     */
    @Test
    public void support() throws Exception {
        UserOperator m0 = extract(Logging.class, Ops.class, "m0");
        UserOperator m1 = extract(Logging.class, Ops.class, "m1");
        UserOperator m2 = extract(Logging.class, Ops.class, "m2");
        UserOperator m3 = extract(Logging.class, Ops.class, "m3");
        UserOperator m4 = extract(Update.class, Ops.class, "m4");
        CoreOperator m5 = CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build();

        assertThat(LoggingOperatorUtil.isSupported(m0), is(true));
        assertThat(LoggingOperatorUtil.isSupported(m1), is(true));
        assertThat(LoggingOperatorUtil.isSupported(m2), is(true));
        assertThat(LoggingOperatorUtil.isSupported(m3), is(true));
        assertThat(LoggingOperatorUtil.isSupported(m4), is(false));
        assertThat(LoggingOperatorUtil.isSupported(m5), is(false));
    }

    /**
     * extract levels.
     * @throws Exception if failed
     */
    @Test
    public void level() throws Exception {
        UserOperator m0 = extract(Logging.class, Ops.class, "m0");
        UserOperator m1 = extract(Logging.class, Ops.class, "m1");
        UserOperator m2 = extract(Logging.class, Ops.class, "m2");
        UserOperator m3 = extract(Logging.class, Ops.class, "m3");

        assertThat(LoggingOperatorUtil.getLogLevel(cl, m0), is(Logging.Level.ERROR));
        assertThat(LoggingOperatorUtil.getLogLevel(cl, m1), is(Logging.Level.WARN));
        assertThat(LoggingOperatorUtil.getLogLevel(cl, m2), is(Logging.Level.INFO));
        assertThat(LoggingOperatorUtil.getLogLevel(cl, m3), is(Logging.Level.DEBUG));
    }

    /**
     * unsupported operator.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void invalid_unsupported() throws Exception {
        LoggingOperatorUtil.getLogLevel(cl, CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build());
    }

    private static UserOperator extract(
            Class<? extends Annotation> annotationType,
            Class<?> operatorClass,
            String methodName) {
        return OperatorExtractor.extract(annotationType, operatorClass, methodName)
                .input("in", typeOf(String.class))
                .output("out", typeOf(String.class))
                .build();
    }

    @SuppressWarnings("javadoc")
    public static abstract class Ops {

        @Logging(Logging.Level.ERROR)
        public abstract void m0();

        @Logging(Logging.Level.WARN)
        public abstract void m1();

        @Logging(Logging.Level.INFO)
        public abstract void m2();

        @Logging(Logging.Level.DEBUG)
        public abstract void m3();

        @Update
        public abstract void m4();
    }
}
