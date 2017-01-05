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
package com.asakusafw.lang.compiler.mapreduce;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.javac.testing.JavaCompiler;
import com.asakusafw.lang.compiler.mapreduce.testing.MapReduceRunner;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Test for {@link CopyStageEmitter}.
 */
public class CopyStageEmitterTest {

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
        CopyStageInfo info = new CopyStageInfo(
                new StageInfo("simple", "simple", "simple"),
                Arrays.asList(new CopyStageInfo.Operation(
                        "out",
                        new SourceInfo(
                                new Path(root, "input/*.txt").toString(),
                                classOf(Text.class),
                                classOf(TextInputFormat.class),
                                Collections.emptyMap()),
                        classOf(TextOutputFormat.class),
                        Collections.emptyMap())),
                base.toString());
        CopyStageEmitter.emit(client, info, javac);
        int status = MapReduceRunner.execute(
                new Configuration(),
                client,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat("exit status code", status, is(0));

        assertThat(collect(MapReduceUtil.getStageOutputPath(base.toString(), "out")), contains("Hello, world!"));
    }

    /**
     * multiple files.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        FileEditor.put(new File(folder.getRoot(), "input/test0.txt"), "Hello0");
        FileEditor.put(new File(folder.getRoot(), "input/test1.txt"), "Hello1");
        FileEditor.put(new File(folder.getRoot(), "input/test2.txt"), "Hello2");

        Path root = new Path(folder.getRoot().toURI());
        Path base = new Path(root, "output");
        ClassDescription client = new ClassDescription("com.example.StageClient");
        CopyStageInfo info = new CopyStageInfo(
                new StageInfo("simple", "simple", "simple"),
                Arrays.asList(new CopyStageInfo.Operation[] {
                        new CopyStageInfo.Operation(
                                "out0",
                                new SourceInfo(
                                        new Path(root, "input/test0.txt").toString(),
                                        classOf(Text.class),
                                        classOf(TextInputFormat.class),
                                        Collections.emptyMap()),
                                classOf(TextOutputFormat.class),
                                Collections.emptyMap()),
                        new CopyStageInfo.Operation(
                                "out1",
                                new SourceInfo(
                                        new Path(root, "input/test1.txt").toString(),
                                        classOf(Text.class),
                                        classOf(TextInputFormat.class),
                                        Collections.emptyMap()),
                                classOf(TextOutputFormat.class),
                                Collections.emptyMap()),
                        new CopyStageInfo.Operation(
                                "out2",
                                new SourceInfo(
                                        new Path(root, "input/test2.txt").toString(),
                                        classOf(Text.class),
                                        classOf(TextInputFormat.class),
                                        Collections.emptyMap()),
                                classOf(TextOutputFormat.class),
                                Collections.emptyMap()),
                }),
                base.toString());
        CopyStageEmitter.emit(client, info, javac);
        int status = MapReduceRunner.execute(
                new Configuration(),
                client,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat("exit status code", status, is(0));

        assertThat(collect(MapReduceUtil.getStageOutputPath(base.toString(), "out0")), contains("Hello0"));
        assertThat(collect(MapReduceUtil.getStageOutputPath(base.toString(), "out1")), contains("Hello1"));
        assertThat(collect(MapReduceUtil.getStageOutputPath(base.toString(), "out2")), contains("Hello2"));
    }

    private List<String> collect(String path) {
        int index = path.lastIndexOf('/');
        assertThat(path, endsWith("*"));
        assertThat(index, is(greaterThan(0)));

        File directory = new File(URI.create(path.substring(0, index)));
        String prefix = path.substring(index + 1, path.length() - 1);
        List<String> results = new ArrayList<>();
        for (File file : directory.listFiles()) {
            if (file.isFile() == false) {
                continue;
            }
            String name = file.getName();
            if (name.startsWith(prefix) == false) {
                continue;
            }
            results.addAll(FileEditor.get(file));
        }
        return results;
    }
}
