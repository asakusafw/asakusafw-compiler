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
package com.asakusafw.bridge.hadoop.temporary;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.mapreduce.testing.InputFormatTester;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.runtime.windows.WindowsConfigurator;

/**
 * Test for {@link TemporaryFileInputFormat}.
 */
public class TemporaryFileInputFormatTest {

    static {
        WindowsConfigurator.install();
    }

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private final Configuration conf = new Configuration();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        File folder = temporary.newFolder();
        File file = new File(folder, "part-testing");
        try (ModelOutput<MockData> out = open(new Path(file.toURI()))) {
            MockData.put(out, "Hello0", "Hello1", "Hello2");
        }
        Map<Integer, String> results = collect(new Path(file.toURI()));
        assertThat(results.keySet(), hasSize(3));
        assertThat(results, hasEntry(0, "Hello0"));
        assertThat(results, hasEntry(1, "Hello1"));
        assertThat(results, hasEntry(2, "Hello2"));
    }

    private ModelOutput<MockData> open(Path path) throws IOException {
        return TemporaryStorage.openOutput(conf, MockData.class, path);
    }

    private Map<Integer, String> collect(Path... expr) throws IOException, InterruptedException {
        TemporaryFileInputFormat.setInputPaths(conf, expr);
        InputFormatTester tester = new InputFormatTester(conf, TemporaryFileInputFormat.class);
        Map<Integer, String> results = new LinkedHashMap<>();
        tester.collect((MockData object) -> results.put(object.getKey(), object.getValue()));
        return results;
    }
}
