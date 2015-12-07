/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.iterative;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;

import org.junit.Test;

import com.asakusafw.lang.compiler.analyzer.OperatorAttributeAnalyzer.AttributeMap;
import com.asakusafw.lang.compiler.analyzer.model.OperatorSource;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.iterative.IterativeExtension;
import com.asakusafw.vocabulary.iterative.Iterative;

/**
 * Test for {@link IterativeOperatorAnalyzer}.
 */
public class IterativeOperatorAnalyzerTest {

    /**
     * scope-less element.
     */
    @Test
    public void global() {
        IterativeExtension extension = get(OperatorKind.USER, Op.GLOBAL);
        assertThat(extension, is(notNullValue()));
        assertThat(extension.isScoped(), is(false));
    }

    /**
     * scoped element.
     */
    @Test
    public void local() {
        IterativeExtension extension = get(OperatorKind.USER, Op.LOCAL);
        assertThat(extension, is(notNullValue()));
        assertThat(extension.isScoped(), is(true));
        assertThat(extension.getParameters(), containsInAnyOrder("a", "b"));
    }

    /**
     * non iterative.
     */
    @Test
    public void nothing() {
        IterativeExtension extension = get(OperatorKind.USER, Op.NOTHING);
        assertThat(extension, is(nullValue()));
    }

    /**
     * iterative external input.
     */
    @Test
    public void external_input() {
        IterativeExtension extension = get(OperatorKind.INPUT, Op.GLOBAL);
        assertThat(extension, is(notNullValue()));
    }

    /**
     * unsupported non-iterative.
     */
    @Test
    public void unsupported_nothing() {
        IterativeExtension extension = get(OperatorKind.MARKER, Op.NOTHING);
        assertThat(extension, is(nullValue()));
    }

    /**
     * non iterative.
     */
    @Test(expected = DiagnosticException.class)
    public void unsupported_iterative() {
        IterativeExtension extension = get(OperatorKind.MARKER, Op.GLOBAL);
        assertThat(extension, is(nullValue()));
    }

    private IterativeExtension get(OperatorKind kind, AnnotatedElement element) {
        IterativeOperatorAnalyzer analyzer = new IterativeOperatorAnalyzer();
        AttributeMap attrs = analyzer.analyze(new OperatorSource(kind, element));
        return attrs.get(IterativeExtension.class);
    }

    private static abstract class Op {

        static final Method NOTHING;

        static final Method GLOBAL;

        static final Method LOCAL;

        static {
            try {
                NOTHING = Op.class.getDeclaredMethod("nothing");
                GLOBAL = Op.class.getDeclaredMethod("global");
                LOCAL = Op.class.getDeclaredMethod("local");
            } catch (ReflectiveOperationException e) {
                throw new AssertionError(e);
            }
        }

        abstract void nothing();

        @Iterative
        abstract void global();

        @Iterative({ "a", "b" })
        abstract void local();
    }
}
