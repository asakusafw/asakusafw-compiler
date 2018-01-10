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
package com.asakusafw.lang.compiler.extension.directio.emitter;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.api.testing.MockDataModelLoader;
import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern;
import com.asakusafw.lang.compiler.extension.externalio.ExternalPortStageInfo;
import com.asakusafw.lang.compiler.javac.testing.JavaCompiler;
import com.asakusafw.lang.compiler.mapreduce.SourceInfo;
import com.asakusafw.lang.compiler.mapreduce.testing.MapReduceRunner;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockDataFormat;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableInputFormat;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableModelInput;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableModelOutput;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.directio.FilePattern;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * Test for {@link OutputStageEmitter}.
 */
public class OutputStageEmitterTest {

    /**
     * Java compiler for testing.
     */
    @Rule
    public final JavaCompiler javac = new JavaCompiler();

    /**
     * Direct I/O context for testing.
     */
    @Rule
    public final DirectIoContext context = new DirectIoContext();

    /**
     * emit map only stage.
     * @throws Exception if failed
     */
    @Test
    public void map_only() throws Exception {
        Map<Integer, String> entries = new LinkedHashMap<>();
        entries.put(100, "A");
        entries.put(200, "B");
        entries.put(300, "C");

        DataModelReference dataModel = MockDataModelLoader.load(MockData.class);
        OutputStageInfo.Operation operation = new OutputStageInfo.Operation(
                "testing",
                dataModel,
                Collections.singletonList(source("testing/input/file.bin", entries)),
                "testing/output",
                OutputPattern.compile(dataModel, "*.bin"),
                Collections.emptyList(),
                classOf(MockDataFormat.class));
        OutputStageInfo info = stage(operation);
        ClassDescription clientClass = OutputStageEmitter.emit(info, javac);
        int status = MapReduceRunner.execute(
                context.newConfiguration(),
                clientClass,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat(status, is(0));

        Map<Integer, String> results = collect(context.file("testing/output"));
        assertThat(results, is(entries));
    }

    /**
     * w/ delete patterns.
     * @throws Exception if failed
     */
    @Test
    public void delete() throws Exception {
        Map<Integer, String> entries = new LinkedHashMap<>();
        entries.put(100, "A");
        entries.put(200, "B");
        entries.put(300, "C");

        DataModelReference dataModel = MockDataModelLoader.load(MockData.class);
        OutputStageInfo.Operation operation = new OutputStageInfo.Operation(
                "testing",
                dataModel,
                Collections.singletonList(source("testing/input/file.bin", entries)),
                "testing/output",
                OutputPattern.compile(dataModel, "*.bin"),
                Arrays.asList(FilePattern.compile("delete/*.bin")),
                classOf(MockDataFormat.class));
        OutputStageInfo info = stage(operation);

        File d0 = context.file("testing/output/delete/d0.bin");
        File d1 = context.file("testing/output/delete/d1.notbin");
        File d2 = context.file("testing/output/notdelete/d1.bin");
        FileEditor.newFile(d0);
        FileEditor.newFile(d1);
        FileEditor.newFile(d2);

        ClassDescription clientClass = OutputStageEmitter.emit(info, javac);
        int status = MapReduceRunner.execute(
                context.newConfiguration(),
                clientClass,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat(status, is(0));

        assertThat(d0.exists(), is(false));
        assertThat(d1.exists(), is(true));
        assertThat(d2.exists(), is(true));

        Map<Integer, String> results = collect(context.file("testing/output"));
        assertThat(results, is(entries));
    }

    /**
     * gather to single file.
     * @throws Exception if failed
     */
    @Test
    public void gather() throws Exception {
        Map<Integer, String> entries = new LinkedHashMap<>();
        entries.put(100, "A");
        entries.put(200, "B");
        entries.put(300, "C");

        DataModelReference dataModel = MockDataModelLoader.load(MockData.class);
        OutputStageInfo.Operation operation = new OutputStageInfo.Operation(
                "testing",
                dataModel,
                Collections.singletonList(source("testing/input/file.bin", entries)),
                "testing/output",
                OutputPattern.compile(dataModel, "single.bin"),
                Collections.emptyList(),
                classOf(MockDataFormat.class));
        OutputStageInfo info = stage(operation);
        ClassDescription clientClass = OutputStageEmitter.emit(info, javac);
        int status = MapReduceRunner.execute(
                context.newConfiguration(),
                clientClass,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat(status, is(0));

        Map<Integer, String> results = collect(context.file("testing/output/single.bin"));
        assertThat(results, is(entries));
    }


    /**
     * gather by property value.
     * @throws Exception if failed
     */
    @Test
    public void properties() throws Exception {
        Map<Integer, String> entries = new LinkedHashMap<>();
        entries.put(100, "A");
        entries.put(200, "B");
        entries.put(300, "A");
        entries.put(400, "B");
        entries.put(500, "A");

        DataModelReference dataModel = MockDataModelLoader.load(MockData.class);
        OutputStageInfo.Operation operation = new OutputStageInfo.Operation(
                "testing",
                dataModel,
                Collections.singletonList(source("testing/input/file.bin", entries)),
                "testing/output",
                OutputPattern.compile(dataModel, "{stringValue}.bin", Arrays.asList("+intValue")),
                Collections.emptyList(),
                classOf(MockDataFormat.class));
        OutputStageInfo info = stage(operation);
        ClassDescription clientClass = OutputStageEmitter.emit(info, javac);
        int status = MapReduceRunner.execute(
                context.newConfiguration(),
                clientClass,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat(status, is(0));

        Map<Integer, String> resultA = collect(context.file("testing/output/A.bin"));
        assertThat(resultA.entrySet(), hasSize(3));
        assertThat(resultA, hasEntry(100, "A"));
        assertThat(resultA, hasEntry(300, "A"));
        assertThat(resultA, hasEntry(500, "A"));

        Map<Integer, String> resultB = collect(context.file("testing/output/B.bin"));
        assertThat(resultB.entrySet(), hasSize(2));
        assertThat(resultB, hasEntry(200, "B"));
        assertThat(resultB, hasEntry(400, "B"));
    }

    /**
     * mixed map-only and gathering.
     * @throws Exception if failed
     */
    @Test
    public void mixed() throws Exception {
        Map<Integer, String> entries = new LinkedHashMap<>();
        entries.put(100, "A");
        entries.put(200, "B");
        entries.put(300, "C");

        DataModelReference dataModel = MockDataModelLoader.load(MockData.class);
        SourceInfo source = source("testing/input/file.bin", entries);
        OutputStageInfo.Operation o0 = new OutputStageInfo.Operation(
                "t0",
                dataModel,
                Collections.singletonList(source),
                "testing/o0",
                OutputPattern.compile(dataModel, "*.bin"),
                Collections.emptyList(),
                classOf(MockDataFormat.class));
        OutputStageInfo.Operation o1 = new OutputStageInfo.Operation(
                "t1",
                dataModel,
                Collections.singletonList(source),
                "testing/o1",
                OutputPattern.compile(dataModel, "single.bin"),
                Collections.emptyList(),
                classOf(MockDataFormat.class));
        OutputStageInfo info = stage(o0, o1);
        ClassDescription clientClass = OutputStageEmitter.emit(info, javac);
        int status = MapReduceRunner.execute(
                context.newConfiguration(),
                clientClass,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat(status, is(0));

        assertThat(collect(context.file("testing/o0")), is(entries));
        assertThat(collect(context.file("testing/o1/single.bin")), is(entries));
    }

    private SourceInfo source(String path, Map<Integer, String> contents) throws IOException {
        try (ModelOutput<MockData> o = WritableModelOutput.create(context.file(path))) {
            MockData.put(o, contents);
        }
        return new SourceInfo(
                context.path(path).toString(),
                classOf(MockData.class),
                classOf(WritableInputFormat.class),
                Collections.emptyMap());
    }

    private Map<Integer, String> collect(File file) throws IOException {
        Map<Integer, String> results = new LinkedHashMap<>();
        if (file.isDirectory()) {
            for (File f : WritableModelInput.collect(file, null, ".bin")) {
                results.putAll(collect(f));
            }
        } else {
            try (ModelInput<MockData> input = WritableModelInput.open(file)) {
                results.putAll(MockData.collect(input));
            }
        }
        return results;
    }

    private OutputStageInfo stage(OutputStageInfo.Operation... operations) {
        return new OutputStageInfo(
                new ExternalPortStageInfo("m", "b", "f", Phase.MAIN),
                Arrays.asList(operations),
                "dummy");
    }
}
