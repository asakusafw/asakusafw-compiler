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
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.junit.Test;

import com.asakusafw.lang.compiler.analyzer.OperatorAttributeAnalyzer.AttributeMap;
import com.asakusafw.lang.compiler.analyzer.model.ConstructorParameter;
import com.asakusafw.lang.compiler.analyzer.model.OperatorSource;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.iterative.IterativeExtension;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;
import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.Import;
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
    public void import_iterative() {
        IterativeExtension extension = get(OperatorKind.INPUT, Ctor.IT);
        assertThat(extension, is(notNullValue()));
    }

    /**
     * non-iterative external input.
     */
    @Test
    public void import_nothing() {
        IterativeExtension extension = get(OperatorKind.INPUT, Ctor.IF);
        assertThat(extension, is(nullValue()));
    }

    /**
     * unsupported non-iterative.
     */
    @Test
    public void export_nothing() {
        IterativeExtension extension = get(OperatorKind.OUTPUT, Ctor.EF);
        assertThat(extension, is(nullValue()));
    }

    /**
     * inherited.
     */
    @Test
    public void user_inherit() {
        IterativeExtension extension = get(OperatorKind.USER, ItOp.NOTHING);
        assertThat(extension, is(notNullValue()));
    }

    /**
     * inherited.
     */
    @Test
    public void user_inherit_override() {
        IterativeExtension extension = get(OperatorKind.USER, ItOp.LOCAL);
        assertThat(extension, is(notNullValue()));
        assertThat(extension.isScoped(), is(true));
        assertThat(extension.getParameters(), containsInAnyOrder("a", "b"));
    }

    /**
     * inherited.
     */
    @Test
    public void import_inherit() {
        IterativeExtension extension = get(OperatorKind.INPUT, Ctor.IF_INHERIT);
        assertThat(extension, is(notNullValue()));
    }

    /**
     * inherited.
     */
    @Test
    public void import_inherit_override() {
        IterativeExtension extension = get(OperatorKind.INPUT, Ctor.IT_INHERIT);
        assertThat(extension, is(notNullValue()));
        assertThat(extension.isScoped(), is(true));
        assertThat(extension.getParameters(), containsInAnyOrder("a", "b"));
    }

    /**
     * external output.
     */
    @Test(expected = DiagnosticException.class)
    public void export_iterative() {
        get(OperatorKind.OUTPUT, Ctor.ET);
    }

    /**
     * external output.
     */
    @Test(expected = DiagnosticException.class)
    public void export_inherit() {
        get(OperatorKind.OUTPUT, Ctor.EF_INHERIT);
    }

    /**
     * marker.
     */
    @Test(expected = DiagnosticException.class)
    public void marker_iterative() {
        get(OperatorKind.MARKER, Op.GLOBAL);
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

    @Iterative
    private static abstract class ItOp {

        static final Method NOTHING;

        static final Method LOCAL;

        static {
            try {
                NOTHING = ItOp.class.getDeclaredMethod("nothing");
                LOCAL = ItOp.class.getDeclaredMethod("local");
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

    private static abstract class Ctor {

        static final Constructor<?> CTOR = Ctor.class.getDeclaredConstructors()[0];
        static final ConstructorParameter IF = new ConstructorParameter(CTOR, 0);
        static final ConstructorParameter IF_INHERIT = new ConstructorParameter(CTOR, 1);
        static final ConstructorParameter IT = new ConstructorParameter(CTOR, 2);
        static final ConstructorParameter IT_INHERIT = new ConstructorParameter(CTOR, 3);
        static final ConstructorParameter EF = new ConstructorParameter(CTOR, 4);
        static final ConstructorParameter EF_INHERIT = new ConstructorParameter(CTOR, 5);
        static final ConstructorParameter ET = new ConstructorParameter(CTOR, 6);

        @SuppressWarnings("unused")
        Ctor(
                @Import(name = "a", description = NoItImporter.class) Object a,
                @Import(name = "b", description = ItImporter.class) Object b,
                @Iterative({"a", "b"}) @Import(name = "c", description = NoItImporter.class) Object c,
                @Iterative({"a", "b"}) @Import(name = "d", description = ItImporter.class) Object d,
                @Export(name = "a", description = NoItExporter.class) Object e,
                @Export(name = "b", description = ItExporter.class) Object f,
                @Iterative({"a", "b"}) @Export(name = "c", description = NoItExporter.class) Object g) {
            return;
        }
    }

    @Iterative
    private static abstract class ItImporter implements ImporterDescription {
        // no special members
    }

    private static abstract class NoItImporter implements ImporterDescription {
        // no special members
    }

    @Iterative
    private static abstract class ItExporter implements ExporterDescription {
        // no special members
    }

    private static abstract class NoItExporter implements ExporterDescription {
        // no special members
    }
}
