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
package com.asakusafw.bridge.hadoop.directio;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.bridge.hadoop.ConfigurationEditor;
import com.asakusafw.bridge.hadoop.InputFormatTester;
import com.asakusafw.bridge.hadoop.InputFormatTester.Collector;
import com.asakusafw.bridge.hadoop.directio.mock.DirectIoContext;
import com.asakusafw.bridge.hadoop.directio.mock.MockData;
import com.asakusafw.bridge.hadoop.directio.mock.MockDataFormat;
import com.asakusafw.bridge.hadoop.directio.mock.WritableModelOutput;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * Test for {@link DirectFileInputFormat}.
 */
public class DirectFileInputFormatTest {

    /**
     * context for testing.
     */
    @Rule
    public DirectIoContext context = new DirectIoContext();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (ModelOutput<MockData> out = WritableModelOutput.create(context.file("a.txt"))) {
            MockData.put(out, "Hello0", "Hello1", "Hello2");
        }
        Configuration conf = conf("/", "*.txt", null, null, null);

        Map<Integer, String> results = collect(conf);
        assertThat(results.keySet(), hasSize(3));
        assertThat(results, hasEntry(0, "Hello0"));
        assertThat(results, hasEntry(1, "Hello1"));
        assertThat(results, hasEntry(2, "Hello2"));
    }

    /**
     * w/o files.
     * @throws Exception if failed
     */
    @Test
    public void missing_files_optional() throws Exception {
        Configuration conf = conf("/", "*.txt", null, "true", null);

        Map<Integer, String> results = collect(conf);
        assertThat(results.keySet(), hasSize(0));
    }

    /**
     * w/o files.
     * @throws Exception if failed
     */
    @Test(expected = IOException.class)
    public void missing_files() throws Exception {
        Configuration conf = conf("/", "*.txt", null, "false", null);

        Map<Integer, String> results = collect(conf);
        assertThat(results.keySet(), hasSize(0));
    }

    private Configuration conf(String basePath, String resourcePath, Class<?> filter, String optional, String args) {
        Configuration conf = context.newConfiguration();
        conf.set(DirectFileInputFormat.KEY_BASE_PATH, basePath);
        conf.set(DirectFileInputFormat.KEY_RESOURCE_PATH, resourcePath);
        conf.set(DirectFileInputFormat.KEY_DATA_CLASS, MockData.class.getName());
        conf.set(DirectFileInputFormat.KEY_FORMAT_CLASS, MockDataFormat.class.getName());
        if (filter != null) {
            conf.set(DirectFileInputFormat.KEY_FILTER_CLASS, filter.getName());
        }
        if (optional != null) {
            conf.set(DirectFileInputFormat.KEY_OPTIONAL, optional);
        }
        ConfigurationEditor.putStageInfo(conf, new StageInfo("u", "b", "f", "s", "e", args));
        return conf;
    }

    private Map<Integer, String> collect(Configuration conf) throws IOException, InterruptedException {
        InputFormatTester tester = new InputFormatTester(conf, DirectFileInputFormat.class);
        final Map<Integer, String> results = new LinkedHashMap<>();
        tester.collect(new Collector<MockData>() {
            @Override
            public void handle(MockData object) {
                results.put(object.getKey(), object.getValue());
            }
        });
        return results;
    }
}
