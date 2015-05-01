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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.javac.testing.JavaCompiler;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Test for {@link CleanupStageEmitter}.
 */
public class CleanupStageEmitterTest {

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
        File a = new File(folder.getRoot(), "a/test.txt");
        File b = new File(folder.getRoot(), "b/test.txt");
        FileEditor.put(a, "Hello, world!");
        FileEditor.put(b, "Hello, world!");

        Path root = new Path(folder.getRoot().toURI());
        Path base = new Path(root, "a");

        ClassDescription client = new ClassDescription("com.example.StageClient");
        CleanupStageInfo info = new CleanupStageInfo(
                new StageInfo("simple", "simple", CleanupStageInfo.DEFAULT_STAGE_ID),
                base.toString());

        CleanupStageEmitter.emit(client, info, javac);
        int status = MapReduceRunner.execute(
                new Configuration(),
                client,
                "testing",
                Collections.<String, String>emptyMap(),
                javac.compile());
        assertThat("exit status code", status, is(0));

        assertThat(a.isFile(), is(false));
        assertThat(b.isFile(), is(true));
    }
}
