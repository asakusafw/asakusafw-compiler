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
package com.asakusafw.lang.compiler.extension.testdriver;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;
import com.asakusafw.lang.compiler.api.testing.MockExporterDescription;
import com.asakusafw.lang.compiler.api.testing.MockExternalPortProcessorContext;
import com.asakusafw.lang.compiler.api.testing.MockImporterDescription;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.extension.testdriver.mock.MockData;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.javac.testing.JavaCompiler;
import com.asakusafw.lang.compiler.mapreduce.testing.MapReduceRunner;
import com.asakusafw.lang.compiler.mapreduce.testing.windows.WindowsConfigurator;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryFileInput;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Test for {@link InternalIoPortProcessor}.
 */
public class InternalIoPortProcessorTest {

    static {
        WindowsConfigurator.install();
    }

    /**
     * temporary folder for testing.
     */
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    /**
     * Java compiler for testing.
     */
    @Rule
    public JavaCompiler javac = new JavaCompiler();

    /**
     * supported.
     */
    @Test
    public void supported() {
        MockExternalPortProcessorContext context = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();
        assertThat(processor.isSupported(context, InternalImporterDescription.Basic.class), is(true));
        assertThat(processor.isSupported(context, InternalExporterDescription.Basic.class), is(true));
        assertThat(processor.isSupported(context, MockImporterDescription.class), is(false));
        assertThat(processor.isSupported(context, MockExporterDescription.class), is(false));
        assertThat(processor.isSupported(context, ImporterDescription.class), is(false));
        assertThat(processor.isSupported(context, ExporterDescription.class), is(false));
    }

    /**
     * input contents.
     */
    @Test
    public void input_contents() {
        MockExternalPortProcessorContext context = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();

        InternalImporterDescription desc = input("testing/input-*");
        ValueDescription value = processor.analyzeInputProperties(context, "t", desc);
        String path = restore(String.class, value);
        assertThat(path, is("testing/input-*"));
    }

    /**
     * output contents.
     */
    @Test
    public void output_contents() {
        MockExternalPortProcessorContext context = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();

        InternalExporterDescription desc = output("testing/output-*");
        ValueDescription value = processor.analyzeOutputProperties(context, "t", desc);
        String path = restore(String.class, value);
        assertThat(path, is("testing/output-*"));
    }

    /**
     * validate - simple case.
     */
    @Test
    public void validate() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("testing/input/0-*"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("testing/output/0-*"));

        MockExternalPortProcessorContext mock = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - input path is bare.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_input_bare() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("0-*"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("testing/output/0-*"));

        MockExternalPortProcessorContext mock = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - input path w/o wildcard.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_input_no_wildcard() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("testing/input/0"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("testing/output/0-*"));

        MockExternalPortProcessorContext mock = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - input path w/o wildcard.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_input_invalid_char() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("testing/input/_-*"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("testing/output/0-*"));

        MockExternalPortProcessorContext mock = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output path is bare.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_bare() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("testing/input/0-*"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("0-*"));

        MockExternalPortProcessorContext mock = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output path w/o wildcard.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_no_wildcard() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("testing/input/0-*"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("testing/output/0"));

        MockExternalPortProcessorContext mock = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output path w/o wildcard.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_invalid_char() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("testing/input/0-*"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("testing/output/_-*"));

        MockExternalPortProcessorContext mock = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output paths are conflict.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_conflict() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("testing/input/0-*"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(
                output("testing/output/a-*"),
                output("testing/output/a-*"));

        MockExternalPortProcessorContext mock = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * process - simple case.
     * @throws Exception if failed
     */
    @Test
    public void process() throws Exception {
        String inputBase = new File(temporary.getRoot(), "internal/input/part").toURI().toString();
        String outputBase = new File(temporary.getRoot(), "internal/output/part").toURI().toString();
        InternalImporterDescription input = input(inputBase + "-*");
        InternalExporterDescription output = output(outputBase + "-*");

        List<ExternalInputReference> inputs = resolve(input);
        List<ExternalOutputReference> outputs = resolve(output);

        MockExternalPortProcessorContext mock = mock();
        InternalIoPortProcessor processor = new InternalIoPortProcessor();
        processor.process(mock, inputs, outputs);

        checkTasks(mock.getTasks(), 1);

        assertThat(inputs.get(0).getPaths(), contains(input.getPathPrefix()));

        Set<String> outputSources = outputs.get(0).getPaths();
        assert outputSources.size() == 1;
        File outputSource = new File(URI.create(outputSources.iterator().next().replace('*', '_')));
        try (ModelOutput<MockData> out = TemporaryStorage.openOutput(
                new Configuration(), MockData.class, FileEditor.create(outputSource))) {
            out.write(new MockData(100, "A"));
            out.write(new MockData(200, "B"));
            out.write(new MockData(300, "C"));
        }

        File classes = javac.compile();
        run(mock, classes, Phase.EPILOGUE);

        Map<Integer, String> results = new LinkedHashMap<>();
        File outputParent = new File(URI.create(outputBase)).getParentFile();
        try (DirectoryStream<Path> s = Files.newDirectoryStream(outputParent.toPath(), "part-*")) {
            for (Path p : s) {
                try (ModelInput<MockData> in = new TemporaryFileInput<>(new FileInputStream(p.toFile()), 0)) {
                    MockData buf = new MockData();
                    while (in.readTo(buf)) {
                        results.put(buf.getKeyOption().get(), buf.getValueOption().getAsString());
                    }
                }
            }
        }
        assertThat(results.keySet(), hasSize(3));
        assertThat(results, hasEntry(100, "A"));
        assertThat(results, hasEntry(200, "B"));
        assertThat(results, hasEntry(300, "C"));
    }

    private MockExternalPortProcessorContext mock() {
        MockExternalPortProcessorContext context = new MockExternalPortProcessorContext(
                CompilerOptions.builder()
                    .withRuntimeWorkingDirectory(new File(temporary.getRoot(), "tempdir").toURI().toString(), false)
                    .build(),
                getClass().getClassLoader(),
                new File(temporary.getRoot(), "output"));
        context.registerExtension(JavaSourceExtension.class, javac);
        return context;
    }

    private List<ExternalInputReference> resolve(InternalImporterDescription... descs) {
        try {
            MockExternalPortProcessorContext mock = mock();
            InternalIoPortProcessor processor = new InternalIoPortProcessor();
            List<ExternalInputReference> results = new ArrayList<>();
            int index = 0;
            for (InternalImporterDescription desc : descs) {
                String name = String.format("p%d", index++);
                ExternalInputInfo info = processor.analyzeInput(mock, name, desc);
                ExternalInputReference resolved = processor.resolveInput(mock, name, info);
                assert resolved.getPaths().size() == 1;
                results.add(resolved);
            }
            return results;
        } catch (DiagnosticException e) {
            throw new AssertionError(e);
        }
    }

    private List<ExternalOutputReference> resolve(InternalExporterDescription... descs) {
        try {
            MockExternalPortProcessorContext mock = mock();
            InternalIoPortProcessor processor = new InternalIoPortProcessor();
            List<ExternalOutputReference> results = new ArrayList<>();
            int index = 0;
            for (InternalExporterDescription desc : descs) {
                String name = String.format("p%d", index++);
                ExternalOutputInfo info = processor.analyzeOutput(mock, name, desc);
                String path = mock.getOptions().getRuntimeWorkingPath(String.format("testing/mock/%s", name));
                ExternalOutputReference resolved = processor.resolveOutput(mock, name, info, Collections.singleton(path));
                results.add(resolved);
            }
            return results;
        } catch (DiagnosticException e) {
            throw new AssertionError(e);
        }
    }

    private Map<String, ExternalInputInfo> resolveToMap(InternalImporterDescription... descs) {
        Map<String, ExternalInputInfo> results = new LinkedHashMap<>();
        for (ExternalInputReference ref : resolve(descs)) {
            results.put(ref.getName(), ref);
        }
        return results;
    }

    private Map<String, ExternalOutputInfo> resolveToMap(InternalExporterDescription... descs) {
        Map<String, ExternalOutputInfo> results = new LinkedHashMap<>();
        for (ExternalOutputReference ref : resolve(descs)) {
            results.put(ref.getName(), ref);
        }
        return results;
    }

    private void checkTasks(TaskReferenceMap tasks, int numberOfEpilogue) {
        assertThat(tasks.getTasks(Phase.INITIALIZE), hasSize(0));
        assertThat(tasks.getTasks(Phase.IMPORT), hasSize(0));
        assertThat(tasks.getTasks(Phase.PROLOGUE), hasSize(0));
        assertThat(tasks.getTasks(Phase.MAIN), hasSize(0));
        assertThat(tasks.getTasks(Phase.EPILOGUE), hasSize(numberOfEpilogue));
        assertThat(tasks.getTasks(Phase.EXPORT), hasSize(0));
        assertThat(tasks.getTasks(Phase.FINALIZE), hasSize(0));
    }

    private <T> T restore(Class<T> type, ValueDescription value) {
        try {
            Object resolved = value.resolve(type.getClassLoader());
            return type.cast(resolved);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private InternalImporterDescription input(String path) {
        return new InternalImporterDescription.Basic(MockData.class, path);
    }

    private InternalExporterDescription output(String path) {
        return new InternalExporterDescription.Basic(MockData.class, path);
    }

    private void run(MockExternalPortProcessorContext mock, File classes, Phase phase) throws Exception {
        Collection<? extends TaskReference> tasks = mock.getTasks().getTasks(phase);
        assertThat(tasks, hasSize(1));
        TaskReference task = tasks.iterator().next();

        assertThat(task, is(instanceOf(HadoopTaskReference.class)));
        HadoopTaskReference hadoop = (HadoopTaskReference) task;
        int status = MapReduceRunner.execute(
                new Configuration(),
                hadoop.getMainClass(),
                "testing",
                Collections.<String, String>emptyMap(),
                classes);
        assertThat(MessageFormat.format(
                "unexpected exit status on {0}",
                phase), status, is(0));
    }
}
