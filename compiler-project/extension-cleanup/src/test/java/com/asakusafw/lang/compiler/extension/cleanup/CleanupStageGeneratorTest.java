/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.cleanup;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext;
import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.javac.testing.JavaCompiler;
import com.asakusafw.lang.compiler.mapreduce.testing.MapReduceRunner;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.runtime.stage.AbstractCleanupStageClient;

/**
 * Test for {@link CleanupStageGenerator}.
 */
public class CleanupStageGeneratorTest {

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
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        File base = temporary.newFolder();

        MockJobflowProcessorContext context = mock(base);
        CleanupStageGenerator processor = new CleanupStageGenerator();
        processor.process(context, new Jobflow(
                new JobflowInfo.Basic("dummy", new ClassDescription("dummy")),
                new OperatorGraph()));

        File file = new File(base, "test.txt");
        File other = temporary.newFile();
        FileEditor.put(file, "Hello, world!");
        assertThat(file.exists(), is(true));
        assertThat(other.exists(), is(true));

        ClassDescription client = new ClassDescription(AbstractCleanupStageClient.IMPLEMENTATION);
        int status = MapReduceRunner.execute(
                new Configuration(),
                client,
                "testing",
                Collections.emptyMap(),
                javac.compile());
        assertThat("exit status code", status, is(0));
        assertThat(file.exists(), is(false));
        assertThat(other.exists(), is(true));
    }

    private MockJobflowProcessorContext mock(File base) {
        MockJobflowProcessorContext context = new MockJobflowProcessorContext(
                CompilerOptions.builder()
                    .withRuntimeWorkingDirectory(base.toURI().toString(), false)
                    .build(),
                getClass().getClassLoader(),
                new File(temporary.getRoot(), "output"));
        context.registerExtension(JavaSourceExtension.class, javac);
        return context;
    }
}
