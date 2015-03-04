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
package com.asakusafw.lang.compiler.mapreduce;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
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
                                Collections.<String, String>emptyMap()),
                        classOf(TextOutputFormat.class),
                        Collections.<String, String>emptyMap())),
                base.toString());
        CopyStageEmitter.emit(client, info, javac);
        int status = MapReduceRunner.execute(
                new Configuration(),
                client,
                "testing",
                Collections.<String, String>emptyMap(),
                javac.compile());
        assertThat("exit status code", status, is(0));
        assertThat(collect("output"), contains("Hello, world!"));
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
}
