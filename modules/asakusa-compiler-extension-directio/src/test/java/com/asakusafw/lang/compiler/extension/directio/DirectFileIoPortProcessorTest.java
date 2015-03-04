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
package com.asakusafw.lang.compiler.extension.directio;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.asakusafw.lang.compiler.extension.directio.mock.DirectIoContext;
import com.asakusafw.lang.compiler.extension.directio.mock.MockData;
import com.asakusafw.lang.compiler.extension.directio.mock.MockDataFormat;
import com.asakusafw.lang.compiler.extension.directio.mock.WritableInputFormat;
import com.asakusafw.lang.compiler.extension.directio.mock.WritableModelInput;
import com.asakusafw.lang.compiler.extension.directio.mock.WritableModelOutput;
import com.asakusafw.lang.compiler.extension.directio.mock.WritableOutputFormat;
import com.asakusafw.lang.compiler.extension.hadoop.HadoopFormatExtension;
import com.asakusafw.lang.compiler.extension.hadoop.HadoopTaskReference;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.javac.testing.JavaCompiler;
import com.asakusafw.lang.compiler.mapreduce.MapReduceRunner;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.vocabulary.directio.DirectFileInputDescription;
import com.asakusafw.vocabulary.directio.DirectFileOutputDescription;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Test {@link DirectFileIoPortProcessor}.
 */
public class DirectFileIoPortProcessorTest {

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
     * Direct I/O testing context.
     */
    @Rule
    public DirectIoContext directio = new DirectIoContext();

    /**
     * supported.
     */
    @Test
    public void supported() {
        MockExternalPortProcessorContext context = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        assertThat(processor.isSupported(context, InputDesc.class), is(true));
        assertThat(processor.isSupported(context, OutputDesc.class), is(true));
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
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();

        InputDesc desc = new InputDesc(MockData.class, "base", "resource", MockDataFormat.class);
        ValueDescription value = processor.analyzeInputProperties(context, "t", desc);
        DirectFileInputModel model = restore(DirectFileInputModel.class, value);
        assertThat(model.getBasePath(), is(desc.getBasePath()));
        assertThat(model.getResourcePattern(), is(desc.getResourcePattern()));
        assertThat(model.getFormatClass(), is(classOf(desc.getFormat())));
        assertThat(model.isOptional(), is(desc.isOptional()));
    }

    /**
     * input contents.
     */
    @Test
    public void input_contents_all() {
        MockExternalPortProcessorContext context = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();

        InputDesc desc = new InputDesc(MockData.class, "base", "resource", MockDataFormat.class)
            .withOptional(true);
        ValueDescription value = processor.analyzeInputProperties(context, "t", desc);
        DirectFileInputModel model = restore(DirectFileInputModel.class, value);
        assertThat(model.getBasePath(), is(desc.getBasePath()));
        assertThat(model.getResourcePattern(), is(desc.getResourcePattern()));
        assertThat(model.getFormatClass(), is(classOf(desc.getFormat())));
        assertThat(model.isOptional(), is(desc.isOptional()));
    }

    /**
     * output contents.
     */
    @Test
    public void output_contents() {
        MockExternalPortProcessorContext context = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();

        OutputDesc desc = new OutputDesc(MockData.class, "base", "resource", MockDataFormat.class);
        ValueDescription value = processor.analyzeOutputProperties(context, "t", desc);
        DirectFileOutputModel model = restore(DirectFileOutputModel.class, value);
        assertThat(model.getBasePath(), is(desc.getBasePath()));
        assertThat(model.getResourcePattern(), is(desc.getResourcePattern()));
        assertThat(model.getOrder(), is(desc.getOrder()));
        assertThat(model.getDeletePatterns(), is(desc.getDeletePatterns()));
        assertThat(model.getFormatClass(), is(classOf(desc.getFormat())));
    }

    /**
     * output contents.
     */
    @Test
    public void output_contents_all() {
        MockExternalPortProcessorContext context = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();

        OutputDesc desc = new OutputDesc(MockData.class, "base", "resource", MockDataFormat.class)
            .withOrder("+intValue", "-stringValue")
            .withDeletePatterns("delete1", "delete2");
        ValueDescription value = processor.analyzeOutputProperties(context, "t", desc);
        DirectFileOutputModel model = restore(DirectFileOutputModel.class, value);
        assertThat(model.getBasePath(), is(desc.getBasePath()));
        assertThat(model.getResourcePattern(), is(desc.getResourcePattern()));
        assertThat(model.getOrder(), is(desc.getOrder()));
        assertThat(model.getDeletePatterns(), is(desc.getDeletePatterns()));
        assertThat(model.getFormatClass(), is(classOf(desc.getFormat())));
    }

    /**
     * validate - simple case.
     */
    @Test
    public void validate() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "c"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * process - simple case.
     * @throws Exception if failed
     */
    @Test
    public void process() throws Exception {
        List<ExternalInputReference> inputs = resolve(input("a", "b"));
        List<ExternalOutputReference> outputs = resolve(output("c", "*.bin"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.process(mock, inputs, outputs);

        checkTasks(mock.getTasks(), 1, 1);

        File classes = javac.compile();
        Map<Integer, String> values = new LinkedHashMap<>();
        values.put(100, "A");
        values.put(200, "B");
        values.put(300, "C");

        prepare("a/b", values);
        run(mock, classes, Phase.PROLOGUE);
        assertThat(collect(inputs.get(0)), is(values));

        prepare(outputs.get(0), values);
        run(mock, classes, Phase.EPILOGUE);
        assertThat(collect("c", null, "bin"), is(values));
    }

    private void prepare(String resource, Map<Integer, String> values) throws IOException {
        prepare(directio.file(resource), values);
    }

    private Map<Integer, String> collect(String directory, String prefix, String suffix) throws IOException {
        File base = directio.file(directory);
        Set<File> files = WritableModelInput.collect(base, prefix, suffix);
        return collect(files);
    }

    private void prepare(ExternalOutputReference port, Map<Integer, String> values) throws IOException {
        assertThat(port.getPaths(), hasSize(1));
        String path = port.getPaths().iterator().next();
        path = path.replace('*', '_');
        File file = new File(URI.create(path));
        prepare(file, values);
    }

    private Map<Integer, String> collect(ExternalInputReference port) throws IOException {
        List<File> files = collectFiles(port.getPaths());
        return collect(files);
    }

    private void prepare(File file, Map<Integer, String> values) throws IOException {
        try (WritableModelOutput<MockData> output = WritableModelOutput.create(file)) {
            MockData.put(output, values);
        }
    }

    private Map<Integer, String> collect(Iterable<File> files) throws IOException {
        Map<Integer, String> results = new LinkedHashMap<>();
        for (File file : files) {
            try (WritableModelInput<MockData> input = WritableModelInput.open(file)) {
                results.putAll(MockData.collect(input));
            }
        }
        return results;
    }

    private List<File> collectFiles(Set<String> paths) {
        List<File> results = new ArrayList<>();
        for (String path : paths) {
            if (path.endsWith("*")) {
                int index = path.lastIndexOf('/');
                assertThat(index, is(greaterThan(0)));
                File base = new File(URI.create(path.substring(0, index)));
                String prefix = path.substring(index + 1, path.length() - 1);
                results.addAll(WritableModelInput.collect(base, prefix, null));
            } else {
                File file = new File(URI.create(path));
                results.add(file);
            }
        }
        return results;
    }

    private MockExternalPortProcessorContext mock() {
        MockExternalPortProcessorContext context = new MockExternalPortProcessorContext(
                CompilerOptions.builder()
                    .withRuntimeWorkingDirectory(new File(temporary.getRoot(), "tempdir").toURI().toString(), false)
                    .build(),
                getClass().getClassLoader(),
                new File(temporary.getRoot(), "output"));
        context.registerExtension(JavaSourceExtension.class, javac);
        context.registerExtension(HadoopFormatExtension.class, new HadoopFormatExtension(
                classOf(WritableInputFormat.class), classOf(WritableOutputFormat.class)));
        return context;
    }

    private <T> T restore(Class<T> type, ValueDescription value) {
        try {
            Object resolved = value.resolve(type.getClassLoader());
            return type.cast(resolved);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private InputDesc input(String basePath, String resourcePattern) {
        return new InputDesc(MockData.class, basePath, resourcePattern, MockDataFormat.class);
    }

    private OutputDesc output(String basePath, String resourcePattern) {
        return new OutputDesc(MockData.class, basePath, resourcePattern, MockDataFormat.class);
    }

    private List<ExternalInputReference> resolve(InputDesc... descs) {
        try {
            MockExternalPortProcessorContext mock = mock();
            DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
            List<ExternalInputReference> results = new ArrayList<>();
            int index = 0;
            for (InputDesc desc : descs) {
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

    private List<ExternalOutputReference> resolve(OutputDesc... descs) {
        try {
            MockExternalPortProcessorContext mock = mock();
            DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
            List<ExternalOutputReference> results = new ArrayList<>();
            int index = 0;
            for (OutputDesc desc : descs) {
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

    private Map<String, ExternalInputInfo> resolveToMap(InputDesc... descs) {
        Map<String, ExternalInputInfo> results = new LinkedHashMap<>();
        for (ExternalInputReference ref : resolve(descs)) {
            results.put(ref.getName(), ref);
        }
        return results;
    }

    private Map<String, ExternalOutputInfo> resolveToMap(OutputDesc... descs) {
        Map<String, ExternalOutputInfo> results = new LinkedHashMap<>();
        for (ExternalOutputReference ref : resolve(descs)) {
            results.put(ref.getName(), ref);
        }
        return results;
    }

    private void checkTasks(TaskReferenceMap tasks, int numberOfPrologue, int numberOfEpilogue) {
        assertThat(tasks.getTasks(Phase.INITIALIZE), hasSize(0));
        assertThat(tasks.getTasks(Phase.IMPORT), hasSize(0));
        assertThat(tasks.getTasks(Phase.PROLOGUE), hasSize(numberOfPrologue));
        assertThat(tasks.getTasks(Phase.MAIN), hasSize(0));
        assertThat(tasks.getTasks(Phase.EPILOGUE), hasSize(numberOfEpilogue));
        assertThat(tasks.getTasks(Phase.EXPORT), hasSize(0));
        assertThat(tasks.getTasks(Phase.FINALIZE), hasSize(0));
    }

    private void run(MockExternalPortProcessorContext mock, File classes, Phase phase) throws Exception {
        Collection<? extends TaskReference> tasks = mock.getTasks().getTasks(phase);
        assertThat(tasks, hasSize(1));
        TaskReference task = tasks.iterator().next();

        assertThat(task, is(instanceOf(HadoopTaskReference.class)));
        HadoopTaskReference hadoop = (HadoopTaskReference) task;
        int status = MapReduceRunner.execute(
                directio.newConfiguration(),
                hadoop.getMainClass(),
                "testing",
                Collections.<String, String>emptyMap(),
                classes);
        assertThat(MessageFormat.format(
                "unexpected exit status on {0}",
                phase), status, is(0));
    }

    private static class InputDesc extends DirectFileInputDescription {

        private final Class<?> modelType;

        private final String basePath;

        private final String resourcePattern;

        private final Class<? extends DataFormat<?>> format;

        private boolean optional;

        InputDesc(
                Class<?> modelType,
                String basePath, String resourcePattern,
                Class<? extends DataFormat<?>> format) {
            this.modelType = modelType;
            this.basePath = basePath;
            this.resourcePattern = resourcePattern;
            this.format = format;
        }

        @Override
        public Class<?> getModelType() {
            return modelType;
        }

        @Override
        public String getBasePath() {
            return basePath;
        }

        @Override
        public String getResourcePattern() {
            return resourcePattern;
        }

        @Override
        public Class<? extends DataFormat<?>> getFormat() {
            return format;
        }

        @Override
        public boolean isOptional() {
            return optional;
        }

        public InputDesc withOptional(boolean newValue) {
            this.optional = newValue;
            return this;
        }
    }

    private static class OutputDesc extends DirectFileOutputDescription {

        private final Class<?> modelType;

        private final String basePath;

        private final String resourcePattern;

        private final Class<? extends DataFormat<?>> format;

        private List<String> deletePatterns = Collections.emptyList();

        private List<String> order = Collections.emptyList();

        OutputDesc(
                Class<?> modelType,
                String basePath, String resourcePattern,
                Class<? extends DataFormat<?>> format) {
            this.modelType = modelType;
            this.basePath = basePath;
            this.resourcePattern = resourcePattern;
            this.format = format;
        }

        @Override
        public Class<?> getModelType() {
            return modelType;
        }

        @Override
        public String getBasePath() {
            return basePath;
        }

        @Override
        public String getResourcePattern() {
            return resourcePattern;
        }

        @Override
        public List<String> getOrder() {
            return order;
        }

        public OutputDesc withOrder(String... newValues) {
            this.order = Arrays.asList(newValues);
            return this;
        }

        @Override
        public List<String> getDeletePatterns() {
            return deletePatterns;
        }

        public OutputDesc withDeletePatterns(String... newValues) {
            this.deletePatterns = Arrays.asList(newValues);
            return this;
        }

        @Override
        public Class<? extends DataFormat<?>> getFormat() {
            return format;
        }
    }
}
