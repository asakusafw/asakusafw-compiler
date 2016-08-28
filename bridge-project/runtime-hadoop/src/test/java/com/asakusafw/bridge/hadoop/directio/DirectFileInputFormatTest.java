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
package com.asakusafw.bridge.hadoop.directio;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.bridge.hadoop.ConfigurationEditor;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.lang.compiler.mapreduce.testing.InputFormatTester;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockDataFormat;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableModelOutput;
import com.asakusafw.runtime.directio.DataFilter;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * Test for {@link DirectFileInputFormat}.
 */
public class DirectFileInputFormatTest {

    /**
     * context for testing.
     */
    @Rule
    public final DirectIoContext context = new DirectIoContext();

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
     * w/ path filter.
     * @throws Exception if failed
     */
    @Test
    public void filter_path() throws Exception {
        try (ModelOutput<MockData> out = WritableModelOutput.create(context.file("a.txt"))) {
            MockData.put(out, 0, "Hello0", "Hello1");
        }
        try (ModelOutput<MockData> out = WritableModelOutput.create(context.file("b.txt"))) {
            MockData.put(out, 2, "Hello2", "Hello3");
        }
        try (ModelOutput<MockData> out = WritableModelOutput.create(context.file("c.txt"))) {
            MockData.put(out, 4, "Hello4", "Hello5");
        }
        Configuration conf = conf("/", "*.txt", MockFilterPath.class, null, "filter=.*b\\.txt");

        Map<Integer, String> results = collect(conf);
        assertThat(results.keySet(), hasSize(4));
        assertThat(results, hasEntry(0, "Hello0"));
        assertThat(results, hasEntry(1, "Hello1"));
        assertThat(results, hasEntry(4, "Hello4"));
        assertThat(results, hasEntry(5, "Hello5"));
    }

    /**
     * w/ object filter.
     * @throws Exception if failed
     */
    @Test
    public void filter_object() throws Exception {
        try (ModelOutput<MockData> out = WritableModelOutput.create(context.file("a.txt"))) {
            MockData.put(out, "Hello0", "Hello1", "Hello2");
        }
        Configuration conf = conf("/", "*.txt", MockFilterObject.class, null, "filter=Hello1");

        Map<Integer, String> results = collect(conf);
        assertThat(results.keySet(), hasSize(2));
        assertThat(results, hasEntry(0, "Hello0"));
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
        Map<Integer, String> results = new LinkedHashMap<>();
        tester.collect((MockData object) -> results.put(object.getKey(), object.getValue()));
        return results;
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
            return pattern.matcher(path).matches() == false;
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
            return pattern.matcher(data.getValue()).matches() == false;
        }
    }
}
