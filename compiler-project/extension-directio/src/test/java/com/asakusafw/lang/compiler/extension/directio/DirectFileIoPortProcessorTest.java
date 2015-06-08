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
import java.util.regex.Pattern;

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
import com.asakusafw.lang.compiler.hadoop.HadoopFormatExtension;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.javac.testing.JavaCompiler;
import com.asakusafw.lang.compiler.mapreduce.testing.MapReduceRunner;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockDataFormat;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableInputFormat;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableModelInput;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableModelOutput;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableOutputFormat;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.runtime.directio.DataFilter;
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
     * validate - dual inputs and outputs.
     */
    @Test
    public void validate_dual() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("i", "0.txt"), input("i", "1.txt"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("o0", "a.txt"), output("o1", "a.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - w/o inputs.
     */
    @Test
    public void validate_wo_inputs() {
        Map<String, ExternalInputInfo> inputs = Collections.emptyMap();
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "c"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - w/o outputs.
     */
    @Test
    public void validate_wo_outputs() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = Collections.emptyMap();

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - input w/ variables.
     */
    @Test
    public void validate_input_variable() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "${input}.txt"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "c"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ property.
     */
    @Test
    public void validate_output_property() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "{stringValue}.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ variable.
     */
    @Test
    public void validate_output_variable() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "${output}.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ wildcard.
     */
    @Test
    public void validate_output_wildcard() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "*.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ order asc.
     */
    @Test
    public void validate_output_order_asc() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "c.txt").withOrder("+intValue"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ order desc.
     */
    @Test
    public void validate_output_order_desc() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "c.txt").withOrder("-intValue"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ order mixed.
     */
    @Test
    public void validate_output_order_mixed() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "c.txt")
                .withOrder("+intValue", "+stringValue"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ delete patterns.
     */
    @Test
    public void validate_output_delete() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "a.txt").withDeletePatterns("*.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ delete patterns.
     */
    @Test
    public void validate_output_delete_many() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "a.txt")
                .withDeletePatterns("*.txt")
                .withDeletePatterns("*.csv")
                .withDeletePatterns("*.md"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - input base path w/ wildcard.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_input_basepath_wildcard() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a/*", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "c"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output base path w/ wildcard.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_basepath_wildcard() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b/*", "c"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - input w/ unrecognized character.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_input_unrecognized() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "?"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "c"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ unrecognized character.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_unrecognized() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "?"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output order w/ unknown property.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_order_unrecognized() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "c").withOrder("+UNKNOWN"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output delete pattern w/ unrecognized character.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_delete_unrecognized() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "c").withDeletePatterns("?"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ both wildcard and property.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_wildcard_property() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "*/{intValue}.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ both wildcard and random.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_wildcard_random() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "*/[1..9].txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output w/ both wildcard and order.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_wildcard_order() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("a", "b"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("b", "*.txt").withOrder("+intValue"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - conflict input/input (OK).
     */
    @Test
    public void validate_conflict_input_input() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("x", "a.txt"), input("x", "b.txt"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("out0", "a.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - conflict input/output.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_conflict_input_output() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("x", "a.txt"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("x", "a.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - conflict output/output.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_conflict_output_output() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("in", "a.txt"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("x", "a.txt"), output("x", "b.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - input contains input (OK).
     */
    @Test
    public void validate_input_contain_input() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("x", "a.txt"), input("x/contain", "a.txt"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("out", "a.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - input contains output (currently OK).
     */
    @Test
    public void validate_input_contain_output() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("x", "a.txt"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("x/contain", "a.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output contains input.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_contain_input() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("x/contain", "a.txt"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("x", "a.txt"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.validate(mock, inputs, outputs);
    }

    /**
     * validate - output contains output.
     */
    @Test(expected = DiagnosticException.class)
    public void validate_output_contain_output() {
        Map<String, ExternalInputInfo> inputs = resolveToMap(input("in", "a.txt"));
        Map<String, ExternalOutputInfo> outputs = resolveToMap(output("x", "a.txt"), output("x/contain", "a.txt"));

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
        List<ExternalInputReference> inputs = resolve(input("in", "a.bin"));
        List<ExternalOutputReference> outputs = resolve(output("out", "a.bin"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.process(mock, inputs, outputs);

        checkTasks(mock.getTasks(), 1, 1);

        File classes = javac.compile();
        Map<Integer, String> values = new LinkedHashMap<>();
        values.put(100, "A");
        values.put(200, "B");
        values.put(300, "C");

        prepare("in/a.bin", values);
        run(mock, classes, Phase.PROLOGUE);
        assertThat(collect(inputs.get(0)), is(values));

        prepare(outputs.get(0), values);
        run(mock, classes, Phase.EPILOGUE);
        assertThat(collect("out/a.bin"), is(values));
    }

    /**
     * process - w/ wildcard.
     * @throws Exception if failed
     */
    @Test
    public void process_wildcard() throws Exception {
        List<ExternalInputReference> inputs = resolve(input("in", "*.bin"));
        List<ExternalOutputReference> outputs = resolve(output("out", "*.bin"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.process(mock, inputs, outputs);

        checkTasks(mock.getTasks(), 1, 1);

        File classes = javac.compile();
        Map<Integer, String> values = new LinkedHashMap<>();
        values.put(100, "A");
        values.put(200, "B");
        values.put(300, "C");

        prepare("in/a.bin", values);
        prepare("in/b.bin", Collections.singletonMap(400, "D"));
        run(mock, classes, Phase.PROLOGUE);

        Map<Integer, String> valuesPlus = new LinkedHashMap<>();
        valuesPlus.putAll(values);
        valuesPlus.put(400, "D");
        assertThat(collect(inputs.get(0)), is(valuesPlus));

        prepare(outputs.get(0), values);
        run(mock, classes, Phase.EPILOGUE);
        assertThat(collect("out", null, ".bin"), is(values));
    }

    /**
     * process - w/ properties.
     * @throws Exception if failed
     */
    @Test
    public void process_property() throws Exception {
        List<ExternalInputReference> inputs = Collections.emptyList();
        List<ExternalOutputReference> outputs = resolve(output("out", "{stringValue}.bin"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.process(mock, inputs, outputs);

        checkTasks(mock.getTasks(), 0, 1);

        File classes = javac.compile();
        Map<Integer, String> values = new LinkedHashMap<>();
        values.put(100, "A");
        values.put(200, "B");
        values.put(300, "A");
        values.put(400, "B");
        values.put(500, "A");

        prepare(outputs.get(0), values);
        run(mock, classes, Phase.EPILOGUE);
        Map<Integer, String> resultA = collect("out/A.bin");
        assertThat(resultA.keySet(), hasSize(3));
        assertThat(resultA, hasEntry(100, "A"));
        assertThat(resultA, hasEntry(300, "A"));
        assertThat(resultA, hasEntry(500, "A"));

        Map<Integer, String> resultB = collect("out/B.bin");
        assertThat(resultB.keySet(), hasSize(2));
        assertThat(resultB, hasEntry(200, "B"));
        assertThat(resultB, hasEntry(400, "B"));
    }

    /**
     * process - w/ order.
     * @throws Exception if failed
     */
    @Test
    public void process_order() throws Exception {
        List<ExternalInputReference> inputs = Collections.emptyList();
        List<ExternalOutputReference> outputs = resolve(output("out", "a.bin").withOrder("-intValue"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.process(mock, inputs, outputs);

        checkTasks(mock.getTasks(), 0, 1);

        File classes = javac.compile();
        Map<Integer, String> values = new LinkedHashMap<>();
        values.put(100, "A");
        values.put(200, "B");
        values.put(300, "C");
        values.put(400, "D");
        values.put(500, "E");

        prepare(outputs.get(0), values);
        run(mock, classes, Phase.EPILOGUE);
        Map<Integer, String> resultA = collect("out/a.bin");
        assertThat(resultA.keySet(), contains(500, 400, 300, 200, 100));
    }

    /**
     * process - w/ delete patterns.
     * @throws Exception if failed
     */
    @Test
    public void process_delete() throws Exception {
        List<ExternalInputReference> inputs = Collections.emptyList();
        List<ExternalOutputReference> outputs = resolve(output("out", "a.bin").withDeletePatterns("*.bin"));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.process(mock, inputs, outputs);

        checkTasks(mock.getTasks(), 0, 1);

        File classes = javac.compile();
        Map<Integer, String> values = new LinkedHashMap<>();
        values.put(100, "A");
        values.put(200, "B");
        values.put(300, "C");

        prepare(outputs.get(0), values);
        prepare("out/b.bin", Collections.singletonMap(0, "?"));
        prepare("out/a.binx", Collections.singletonMap(0, "?"));
        run(mock, classes, Phase.EPILOGUE);
        assertThat(collect("out", null, ".bin"), is(values));

        assertThat(directio.file("out/a.bin").exists(), is(true));
        assertThat(directio.file("out/b.bin").exists(), is(false));
        assertThat(directio.file("out/a.binx").exists(), is(true));
    }

    /**
     * process - w/ path filter.
     * @throws Exception if failed
     */
    @Test
    public void process_filter_path() throws Exception {
        List<ExternalInputReference> inputs = resolve(input("in", "*.bin")
                .withFilter(MockFilterPath.class));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.process(mock, inputs, Collections.<ExternalOutputReference>emptyList());

        checkTasks(mock.getTasks(), 1, 0);

        File classes = javac.compile();

        prepare("in/a.bin", Collections.singletonMap(100, "A"));
        prepare("in/b.bin", Collections.singletonMap(200, "B"));
        prepare("in/c.bin", Collections.singletonMap(300, "C"));
        prepare("in/d.bin", Collections.singletonMap(400, "D"));
        run(mock, classes, Phase.PROLOGUE, Collections.singletonMap("filter", ".*/[ac]\\.bin"));

        Map<Integer, String> results = collect(inputs.get(0));
        assertThat(results.keySet(), hasSize(2));
        assertThat(results, hasEntry(100, "A"));
        assertThat(results, hasEntry(300, "C"));
    }

    /**
     * process - w/ data filter.
     * @throws Exception if failed
     */
    @Test
    public void process_filter_object() throws Exception {
        List<ExternalInputReference> inputs = resolve(input("in", "*.bin")
                .withFilter(MockFilterObject.class));

        MockExternalPortProcessorContext mock = mock();
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.process(mock, inputs, Collections.<ExternalOutputReference>emptyList());

        checkTasks(mock.getTasks(), 1, 0);

        File classes = javac.compile();

        prepare("in/a.bin", Collections.singletonMap(100, "A"));
        prepare("in/b.bin", Collections.singletonMap(200, "B"));
        prepare("in/c.bin", Collections.singletonMap(300, "C"));
        prepare("in/d.bin", Collections.singletonMap(400, "D"));
        run(mock, classes, Phase.PROLOGUE, Collections.singletonMap("filter", "[BC]"));

        Map<Integer, String> results = collect(inputs.get(0));
        assertThat(results.keySet(), hasSize(2));
        assertThat(results, hasEntry(200, "B"));
        assertThat(results, hasEntry(300, "C"));
    }

    /**
     * process - w/ path filter but is disabled.
     * @throws Exception if failed
     */
    @Test
    public void process_filter_path_disabled() throws Exception {
        List<ExternalInputReference> inputs = resolve(input("in", "*.bin")
                .withFilter(MockFilterPath.class));

        MockExternalPortProcessorContext mock = mock(
                Collections.singletonMap(DirectFileIoPortProcessor.OPTION_FILTER_ENABLED, String.valueOf(false)));
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.process(mock, inputs, Collections.<ExternalOutputReference>emptyList());

        checkTasks(mock.getTasks(), 1, 0);

        File classes = javac.compile();

        prepare("in/a.bin", Collections.singletonMap(100, "A"));
        prepare("in/b.bin", Collections.singletonMap(200, "B"));
        prepare("in/c.bin", Collections.singletonMap(300, "C"));
        prepare("in/d.bin", Collections.singletonMap(400, "D"));
        run(mock, classes, Phase.PROLOGUE, Collections.singletonMap("filter", ".*/[ac]\\.bin"));

        Map<Integer, String> results = collect(inputs.get(0));
        assertThat(results.keySet(), hasSize(4));
        assertThat(results, hasEntry(100, "A"));
        assertThat(results, hasEntry(200, "B"));
        assertThat(results, hasEntry(300, "C"));
        assertThat(results, hasEntry(400, "D"));
    }

    /**
     * process - w/ data filter but is disabled.
     * @throws Exception if failed
     */
    @Test
    public void process_filter_object_disabled() throws Exception {
        List<ExternalInputReference> inputs = resolve(input("in", "*.bin")
                .withFilter(MockFilterObject.class));

        MockExternalPortProcessorContext mock = mock(
                Collections.singletonMap(DirectFileIoPortProcessor.OPTION_FILTER_ENABLED, String.valueOf(false)));
        DirectFileIoPortProcessor processor = new DirectFileIoPortProcessor();
        processor.process(mock, inputs, Collections.<ExternalOutputReference>emptyList());

        checkTasks(mock.getTasks(), 1, 0);

        File classes = javac.compile();

        prepare("in/a.bin", Collections.singletonMap(100, "A"));
        prepare("in/b.bin", Collections.singletonMap(200, "B"));
        prepare("in/c.bin", Collections.singletonMap(300, "C"));
        prepare("in/d.bin", Collections.singletonMap(400, "D"));
        run(mock, classes, Phase.PROLOGUE, Collections.singletonMap("filter", "[BC]"));

        Map<Integer, String> results = collect(inputs.get(0));
        assertThat(results.keySet(), hasSize(4));
        assertThat(results, hasEntry(100, "A"));
        assertThat(results, hasEntry(200, "B"));
        assertThat(results, hasEntry(300, "C"));
        assertThat(results, hasEntry(400, "D"));
    }

    private void prepare(String resource, Map<Integer, String> values) throws IOException {
        prepare(directio.file(resource), values);
    }

    private Map<Integer, String> collect(String resource) throws IOException {
        File file = directio.file(resource);
        return collect(Arrays.asList(file));
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
        return mock(Collections.<String, String>emptyMap());
    }

    private MockExternalPortProcessorContext mock(Map<String, String> options) {
        MockExternalPortProcessorContext context = new MockExternalPortProcessorContext(
                CompilerOptions.builder()
                    .withRuntimeWorkingDirectory(new File(temporary.getRoot(), "tempdir").toURI().toString(), false)
                    .withProperties(options)
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
        run(mock, classes, phase, Collections.<String, String>emptyMap());
    }

    private void run(
            MockExternalPortProcessorContext mock,
            File classes,
            Phase phase,
            Map<String, String> arguments) throws Exception {
        Collection<? extends TaskReference> tasks = mock.getTasks().getTasks(phase);
        assertThat(tasks, hasSize(1));
        TaskReference task = tasks.iterator().next();

        assertThat(task, is(instanceOf(HadoopTaskReference.class)));
        HadoopTaskReference hadoop = (HadoopTaskReference) task;
        int status = MapReduceRunner.execute(
                directio.newConfiguration(),
                hadoop.getMainClass(),
                "testing",
                arguments,
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

        private Class<? extends DataFilter<?>> filter;

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

        @Override
        public Class<? extends DataFilter<?>> getFilter() {
            return filter;
        }

        public InputDesc withFilter(Class<? extends DataFilter<?>> newValue) {
            this.filter = newValue;
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

    /**
     * filters by path.
     */
    public static class MockFilterPath extends DataFilter<Object> {

        private Pattern pattern;

        @Override
        public void initialize(DataFilter.Context context) {
            pattern = Pattern.compile(context.getBatchArguments().get("filter"));
        }

        @Override
        public boolean acceptsPath(String path) {
            return pattern.matcher(path).matches();
        }
    }

    /**
     * filters by object.
     */
    public static class MockFilterObject extends DataFilter<MockData> {

        private Pattern pattern;

        @Override
        public void initialize(DataFilter.Context context) {
            pattern = Pattern.compile(context.getBatchArguments().get("filter"));
        }

        @Override
        public boolean acceptsData(MockData data) {
            return pattern.matcher(data.getStringValueOption().getAsString()).matches();
        }
    }
}
