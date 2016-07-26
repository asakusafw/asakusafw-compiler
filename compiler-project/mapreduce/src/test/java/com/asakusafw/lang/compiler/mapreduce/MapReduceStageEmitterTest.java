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
package com.asakusafw.lang.compiler.mapreduce;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.javac.testing.JavaCompiler;
import com.asakusafw.lang.compiler.mapreduce.testing.MapReduceRunner;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.stage.output.StageOutputDriver;
import com.asakusafw.runtime.stage.resource.StageResourceDriver;

/**
 * Test for {@link MapReduceStageEmitter}.
 */
public class MapReduceStageEmitterTest {

    /**
     * Java compiler.
     */
    @Rule
    public final JavaCompiler javac = new JavaCompiler();

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        FileEditor.put(new File(folder.getRoot(), "input/test.txt"), "Hello, world!");
        Path root = new Path(folder.getRoot().toURI());
        Path base = new Path(root, "output");
        ClassDescription client = new ClassDescription("com.example.StageClient");
        MapReduceStageInfo info = new MapReduceStageInfo(
                new StageInfo("simple", "simple", "simple"),
                Arrays.asList(new MapReduceStageInfo.Input(
                        new Path(root, "input/*.txt").toString(),
                        classOf(Text.class),
                        classOf(TextInputFormat.class),
                        classOf(SimpleMapper.class),
                        Collections.emptyMap())),
                Arrays.asList(new MapReduceStageInfo.Output(
                        "out",
                        classOf(NullWritable.class),
                        classOf(Text.class),
                        classOf(TextOutputFormat.class),
                        Collections.emptyMap())),
                Collections.emptyList(),
                base.toString());
        MapReduceStageEmitter.emit(client, info, javac);
        int status = MapReduceRunner.execute(
                new Configuration(),
                client,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat("exit status code", status, is(0));
        assertThat(collect("output"), contains("Hello, world!"));
    }

    /**
     * w/ reduce.
     * @throws Exception if failed
     */
    @Test
    public void reduce() throws Exception {
        FileEditor.put(new File(folder.getRoot(), "input/test.txt"), "Hello, world!");
        Path root = new Path(folder.getRoot().toURI());
        Path base = new Path(root, "output");
        ClassDescription client = new ClassDescription("com.example.StageClient");
        MapReduceStageInfo info = new MapReduceStageInfo(
                new StageInfo("simple", "simple", "simple"),
                Arrays.asList(new MapReduceStageInfo.Input(
                        new Path(root, "input/*.txt").toString(),
                        classOf(Text.class),
                        classOf(TextInputFormat.class),
                        classOf(Mapper.class),
                        Collections.emptyMap())),
                Arrays.asList(new MapReduceStageInfo.Output(
                        "out",
                        classOf(NullWritable.class),
                        classOf(Text.class),
                        classOf(TextOutputFormat.class),
                        Collections.emptyMap())),
                Collections.emptyList(),
                new MapReduceStageInfo.Shuffle(
                        classOf(LongWritable.class),
                        classOf(Text.class),
                        classOf(HashPartitioner.class),
                        null,
                        classOf(LongWritable.Comparator.class),
                        classOf(LongWritable.Comparator.class),
                        classOf(SimpleReducer.class)),
                base.toString());
        MapReduceStageEmitter.emit(client, info, javac);
        int status = MapReduceRunner.execute(
                new Configuration(),
                client,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat("exit status code", status, is(0));
        assertThat(collect("output"), contains("Hello, world!"));
    }

    /**
     * w/ resources.
     * @throws Exception if failed
     */
    @Test
    public void resource() throws Exception {
        FileEditor.put(new File(folder.getRoot(), "input/test.txt"), "Hello, input!");
        FileEditor.put(new File(folder.getRoot(), "resource/test.txt"), "Hello, resource!");
        Path root = new Path(folder.getRoot().toURI());
        Path base = new Path(root, "output");
        ClassDescription client = new ClassDescription("com.example.StageClient");
        MapReduceStageInfo info = new MapReduceStageInfo(
                new StageInfo("simple", "simple", "simple"),
                Arrays.asList(new MapReduceStageInfo.Input(
                        new Path(root, "input/*.txt").toString(),
                        classOf(Text.class),
                        classOf(TextInputFormat.class),
                        classOf(ResourceMapper.class),
                        Collections.emptyMap())),
                Arrays.asList(new MapReduceStageInfo.Output(
                        "out",
                        classOf(NullWritable.class),
                        classOf(Text.class),
                        classOf(TextOutputFormat.class),
                        Collections.emptyMap())),
                Arrays.asList(new MapReduceStageInfo.Resource(
                        new Path(root, "resource/*.txt").toString(),
                        "resource")),
                base.toString());
        MapReduceStageEmitter.emit(client, info, javac);
        int status = MapReduceRunner.execute(
                new Configuration(),
                client,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat("exit status code", status, is(0));
        assertThat(collect("output"), contains("Hello, resource!"));
    }

    private List<String> collect(String path) {
        List<String> results = new ArrayList<>();
        File directory = new File(folder.getRoot(), path);
        for (File file : directory.listFiles()) {
            if (file.isFile() == false) {
                continue;
            }
            String name = file.getName();
            if (name.startsWith(".") || name.startsWith("_")) {
                continue;
            }
            results.addAll(FileEditor.get(file));
        }
        return results;
    }

    @SuppressWarnings("javadoc")
    public static class SimpleMapper<T> extends Mapper<Object, T, Object, T> {

        private StageOutputDriver output;

        private Result<T> result;

        @SuppressWarnings("unchecked")
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.output = new StageOutputDriver(context);
            this.result = (Result<T>) output.getResultSink("out");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.output.close();
            this.output = null;
            this.result = null;
        }

        @Override
        protected void map(Object key, T value, Context context) throws IOException, InterruptedException {
            result.add(value);
        }
    }

    @SuppressWarnings("javadoc")
    public static class SimpleReducer<T> extends Reducer<Object, T, Object, T> {

        private StageOutputDriver output;

        private Result<T> result;

        @SuppressWarnings("unchecked")
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.output = new StageOutputDriver(context);
            this.result = (Result<T>) output.getResultSink("out");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.output.close();
            this.output = null;
            this.result = null;
        }

        @Override
        protected void reduce(Object key, Iterable<T> values, Context context) throws IOException, InterruptedException {
            for (T value : values) {
                result.add(value);
            }
        }
    }

    @SuppressWarnings("javadoc")
    public static class ResourceMapper extends Mapper<Object, Text, Object, Text> {

        private StageOutputDriver output;

        private Result<Text> result;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.output = new StageOutputDriver(context);
            this.result = output.getResultSink("out");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.output.close();
            this.output = null;
            this.result = null;
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            try {
                body(context);
            } finally {
                cleanup(context);
            }
        }

        private void body(Context context) throws IOException {
            Text buf = new Text();
            LocalFileSystem fs = FileSystem.getLocal(context.getConfiguration());
            try (StageResourceDriver driver = new StageResourceDriver(context.getConfiguration())) {
                for (Path path : driver.findCache("resource")) {
                    File file = fs.pathToFile(path);
                    List<String> lines = FileEditor.get(file);
                    for (String line : lines) {
                        buf.set(line);
                        result.add(buf);
                    }
                }
            }
        }
    }
}
