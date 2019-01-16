/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.mapreduce.testing;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.runtime.stage.AbstractStageClient;
import com.asakusafw.runtime.stage.StageInput;
import com.asakusafw.runtime.stage.StageOutput;
import com.asakusafw.runtime.stage.preparator.PreparatorMapper;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link MapReduceRunner}.
 */
public class MapReduceRunnerTest {

    /**
     * Support for Windows platform.
     */
    @ClassRule
    public static final WindowsSupport WINDOWS_SUPPORT = new WindowsSupport();

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
        SimpleClient.BASE.set(new Path(folder.getRoot().toURI()));
        int status = MapReduceRunner.execute(
                new Configuration(),
                Descriptions.classOf(SimpleClient.class),
                "testing",
                Collections.emptyMap());
        assertThat("exit status code", status, is(0));
        assertThat(collect("output"), contains("Hello, world!"));
    }

    /**
     * client class is not found.
     * @throws Exception if failed
     */
    @Test(expected = Exception.class)
    public void missing_class() throws Exception {
        MapReduceRunner.execute(
                new Configuration(),
                new ClassDescription("___MISSING___"),
                "testing",
                Collections.emptyMap());
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
    public static class SimpleClient extends AbstractStageClient {

        static final AtomicReference<Path> BASE = new AtomicReference<>();

        private static Path getBase() {
            Path path = BASE.get();
            if (path == null) {
                throw new AssertionError();
            }
            return path;
        }

        @Override
        protected String getStageOutputPath() {
            return new Path(getBase(), "output").toString();
        }

        @Override
        protected List<StageInput> getStageInputs() {
            StageInput result = new StageInput(
                    new Path(getBase(), "input/*.txt").toString(),
                    TextInputFormat.class,
                    SimpleMapper.class);
            return Collections.singletonList(result);
        }

        @Override
        protected List<StageOutput> getStageOutputs() {
            StageOutput result = new StageOutput(
                    "out",
                    NullWritable.class,
                    Text.class,
                    TextOutputFormat.class);
            return Collections.singletonList(result);
        }

        @Override
        protected String getBatchId() {
            return "simple";
        }

        @Override
        protected String getFlowId() {
            return "simple";
        }

        @Override
        protected String getStageId() {
            return "simple";
        }
    }

    @SuppressWarnings("javadoc")
    public static class SimpleMapper extends PreparatorMapper<Object> {

        @Override
        public String getOutputName() {
            return "out";
        }
    }
}
