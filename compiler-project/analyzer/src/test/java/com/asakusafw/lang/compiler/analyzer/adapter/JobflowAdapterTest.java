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
package com.asakusafw.lang.compiler.analyzer.adapter;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Constructor;
import java.util.Arrays;

import org.junit.Test;

import com.asakusafw.lang.compiler.analyzer.mock.AbstractJobflow;
import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithMultiConstructor;
import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithWrongName;
import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithoutAnnotation;
import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithoutDescription;
import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithoutValidConstructor;
import com.asakusafw.lang.compiler.analyzer.mock.MockExporterDescription;
import com.asakusafw.lang.compiler.analyzer.mock.MockImporterDescription;
import com.asakusafw.lang.compiler.analyzer.mock.MockJobflow;
import com.asakusafw.lang.compiler.analyzer.mock.NotPublicAccessor;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.JobFlow;
import com.asakusafw.vocabulary.flow.Out;

/**
 * Test for {@link JobflowAdapter}.
 */
public class JobflowAdapterTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        assertThat(JobflowAdapter.isJobflow(MockJobflow.class), is(true));

        JobflowAdapter adapter = JobflowAdapter.analyze(MockJobflow.class);
        JobflowInfo info = adapter.getInfo();
        assertThat(adapter.toString(), info.getFlowId(), is("mock"));
        assertThat(info.getDescriptionClass(), is(Descriptions.classOf(MockJobflow.class)));

        assertThat(adapter.getDescription(), is((Object) MockJobflow.class));
        assertThat(adapter.getConstructor().getDeclaringClass(), is((Object) MockJobflow.class));
        assertThat(adapter.getConstructor().getParameterTypes().length, is(2));
        assertThat(adapter.getParameters(), hasSize(2));

        JobflowAdapter.Parameter p0 = adapter.getParameters().get(0);
        assertThat(p0.getName(), is("in"));
        assertThat(p0.getDirection(), is(JobflowAdapter.Direction.INPUT));
        assertThat(p0.getDataModelClass(), is((Object) String.class));
        assertThat(p0.getDescriptionClass(), is((Object) MockImporterDescription.class));

        JobflowAdapter.Parameter p1 = adapter.getParameters().get(1);
        assertThat(p1.getName(), is("out"));
        assertThat(p1.getDirection(), is(JobflowAdapter.Direction.OUTPUT));
        assertThat(p1.getDataModelClass(), is((Object) String.class));
        assertThat(p1.getDescriptionClass(), is((Object) MockExporterDescription.class));

        FlowDescription instance = adapter.newInstance(Arrays.asList(null, null));
        assertThat(instance, is(instanceOf(MockJobflow.class)));
    }

    /**
     * jobflow class must be annotated.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_wo_annotation() {
        assertThat(JobflowAdapter.isJobflow(JobflowWithoutAnnotation.class), is(false));
        JobflowAdapter.analyze(JobflowWithoutAnnotation.class);
    }

    /**
     * jobflow class must inherit {@link FlowDescription}.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_wo_description() {
        assertThat(JobflowAdapter.isJobflow(JobflowWithoutDescription.class), is(false));
        JobflowAdapter.analyze(JobflowWithoutDescription.class);
    }

    /**
     * jobflow class must have valid ID.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_w_wrong_name() {
        JobflowAdapter.analyze(JobflowWithWrongName.class);
    }

    /**
     * jobflow class must be top-level.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_not_top_level() {
        JobflowAdapter.analyze(NotTopLevelJobflow.class);
    }

    /**
     * jobflow class must be public.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_not_public() {
        JobflowAdapter.analyze(NotPublicAccessor.getJobflow());
    }

    /**
     * jobflow class must not be abstract.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_abstract() {
        JobflowAdapter.analyze(AbstractJobflow.class);
    }

    /**
     * jobflow class must have just one public constructor.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_multi_ctor() {
        JobflowAdapter.analyze(JobflowWithMultiConstructor.class);
    }

    /**
     * jobflow class must have at least one public constructor.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_wo_ctor() {
        JobflowAdapter.analyze(JobflowWithoutValidConstructor.class);
    }

    /**
     * jobflow constructor must have at least one inputs.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_wo_inputs() {
        JobflowAdapter.analyzeParameters(find(InvalidIoCtors.class, "empty_inputs"));
    }

    /**
     * jobflow constructor must have at least one outputs.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_wo_outputs() {
        JobflowAdapter.analyzeParameters(find(InvalidIoCtors.class, "empty_outputs"));
    }

    /**
     * each jobflow input name must be unique.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_conflict_inputs() {
        JobflowAdapter.analyzeParameters(find(InvalidIoCtors.class, "conflict_inputs"));
    }

    /**
     * each jobflow output name must be unique.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_conflict_outputs() {
        JobflowAdapter.analyzeParameters(find(InvalidIoCtors.class, "conflict_outputs"));
    }

    /**
     * each jobflow input name must be valid identifier.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_input_name() {
        JobflowAdapter.analyzeParameters(find(InvalidNameCtors.class, "invalid_input"));
    }

    /**
     * each jobflow output name must be valid identifier.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_output_name() {
        JobflowAdapter.analyzeParameters(find(InvalidNameCtors.class, "invalid_output"));
    }

    /**
     * each jobflow parameter must be either {@code In} or {@code Out}.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_unknown_type() {
        JobflowAdapter.analyzeParameters(find(InvalidTypeCtors.class, "unknown"));
    }

    /**
     * each jobflow parameter must be parameterized.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_raw_input() {
        JobflowAdapter.analyzeParameters(find(InvalidTypeCtors.class, "raw"));
    }

    /**
     * each jobflow input must have importer description.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_input_wo_description() {
        JobflowAdapter.analyzeParameters(find(InvalidTypeCtors.class, "no_import"));
    }

    /**
     * each jobflow output must have importer description.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_output_wo_description() {
        JobflowAdapter.analyzeParameters(find(InvalidTypeCtors.class, "no_export"));
    }

    /**
     * each jobflow parameter.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_ctor_parameter_redundant_annotation() {
        JobflowAdapter.analyzeParameters(find(InvalidTypeCtors.class, "redundant"));
    }

    @SuppressWarnings("unchecked")
    static Constructor<? extends FlowDescription> find(Class<?> aClass, String id) {
        for (Constructor<?> ctor : aClass.getDeclaredConstructors()) {
            Id a = ctor.getAnnotation(Id.class);
            if (a != null && a.value().equals(id)) {
                return (Constructor<? extends FlowDescription>) ctor;
            }
        }
        throw new AssertionError(id);
    }

    @SuppressWarnings("javadoc")
    @JobFlow(name = "NotTopLevelJobflow")
    public static final class NotTopLevelJobflow extends FlowDescription {
        @Override
        protected void describe() {
            return;
        }
    }

    @SuppressWarnings("javadoc")
    public static final class Ctors extends FlowDescription {

        @Id("ok")
        public Ctors(
                @Import(name = "in", description = MockImporterDescription.class) In<String> in,
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out) {
            return;
        }

        @Override
        protected void describe() {
            return;
        }
    }

    @SuppressWarnings("javadoc")
    public static final class InvalidIoCtors extends FlowDescription {

        @Id("empty_inputs")
        public InvalidIoCtors(
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out) {
            return;
        }

        @Id("empty_outputs")
        public InvalidIoCtors(
                @Import(name = "in", description = MockImporterDescription.class) In<String> in) {
            return;
        }

        @Id("conflict_inputs")
        public InvalidIoCtors(
                @Import(name = "in", description = MockImporterDescription.class) In<String> in1,
                @Import(name = "in", description = MockImporterDescription.class) In<String> in2,
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out) {
            return;
        }

        @Id("conflict_outputs")
        public InvalidIoCtors(
                @Import(name = "in", description = MockImporterDescription.class) In<String> in,
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out1,
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out2) {
            return;
        }

        @Override
        protected void describe() {
            return;
        }
    }

    @SuppressWarnings("javadoc")
    public static final class InvalidNameCtors extends FlowDescription {

        @Id("invalid_input")
        public InvalidNameCtors(
                @Import(name = "in", description = MockImporterDescription.class) In<String> in1,
                @Import(name = "$INVALID", description = MockImporterDescription.class) In<String> in2,
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out) {
            return;
        }

        @Id("invalid_output")
        public InvalidNameCtors(
                @Import(name = "in", description = MockImporterDescription.class) In<String> in,
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out1,
                @Export(name = "$INVALID", description = MockExporterDescription.class) Out<String> out2) {
            return;
        }

        @Override
        protected void describe() {
            return;
        }
    }

    @SuppressWarnings("javadoc")
    public static final class InvalidTypeCtors extends FlowDescription {

        @Id("unknown")
        public InvalidTypeCtors(
                @Import(name = "in", description = MockImporterDescription.class) In<String> in,
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out,
                String unknown) {
            return;
        }

        @SuppressWarnings("rawtypes")
        @Id("raw")
        public InvalidTypeCtors(
                @Import(name = "in", description = MockImporterDescription.class) In<String> in,
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out,
                @Import(name = "rawin", description = MockImporterDescription.class) In rawIn,
                @Export(name = "rawout", description = MockExporterDescription.class) Out rawOut) {
            return;
        }

        @Id("no_import")
        public InvalidTypeCtors(
                @Import(name = "in", description = MockImporterDescription.class) In<String> in,
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out,
                In<String> input) {
            return;
        }

        @Id("no_export")
        public InvalidTypeCtors(
                @Import(name = "in", description = MockImporterDescription.class) In<String> in,
                @Export(name = "out", description = MockExporterDescription.class) Out<String> out,
                Out<String> output) {
            return;
        }

        @Id("redundant")
        public InvalidTypeCtors(
                @Import(name = "in", description = MockImporterDescription.class)
                @Export(name = "out", description = MockExporterDescription.class)
                In<String> in,
                @Import(name = "in", description = MockImporterDescription.class)
                @Export(name = "out", description = MockExporterDescription.class)
                Out<String> out) {
            return;
        }

        @Override
        protected void describe() {
            return;
        }
    }

}
