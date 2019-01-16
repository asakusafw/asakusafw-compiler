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
package com.asakusafw.dag.runtime.directio;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.testing.VertexProcessorRunner;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockDataFormat;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableModelInput;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link DirectFileOutputPrepare}.
 */
public class DirectFileOutputPrepareTest {

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * Support for Windows platform.
     */
    @ClassRule
    public static final WindowsSupport WINDOWS_SUPPORT = new WindowsSupport();

    /**
     * Direct I/O testing context.
     */
    @Rule
    public final DirectIoContext directio = new DirectIoContext();

    private Configuration configuration;

    /**
     * Set up.
     */
    @Before
    public void setup() {
        configuration = directio.newConfiguration();
    }

    /**
     * simple case - flat.
     */
    @Test
    public void flat() {
        flat(p -> p.bind("a", "out", "*.bin", MockDataFormat.class),
                new MockData().set(0, "Hello, world!"));
        Map<Integer, String> results = collect("out", "", ".bin");
        assertThat(results.keySet(), hasSize(1));
        assertThat(results, hasEntry(0, "Hello, world!"));
    }

    /**
     * simple case - group.
     */
    @Test
    public void group() {
        group(p -> p.bind("a", "out", null, MockDataFormat.class),
                new MockData().set(100, "Hello, world!"));
        Map<Integer, String> results = collect("out/100.bin");
        assertThat(results.keySet(), hasSize(1));
        assertThat(results, hasEntry(100, "Hello, world!"));
    }

    /**
     * flat - no sources.
     */
    @Test
    public void flat_empty() {
        flat(p -> p.bind("a", "out", "*.bin", MockDataFormat.class));
        Map<Integer, String> results = collect("out", "", ".bin");
        assertThat(results.keySet(), hasSize(0));
        assertThat(directio.file("out").exists(), is(false));
    }

    private Map<Integer, String> collect(String path) {
        try (ModelInput<MockData> in = WritableModelInput.open(directio.file(path))) {
            return MockData.collect(in);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private Map<Integer, String> collect(String base, String prefix, String suffix) {
        File dir = directio.file(base);
        Set<File> files = WritableModelInput.collect(dir, prefix, suffix);
        Map<Integer, String> results = new LinkedHashMap<>();
        for (File f : files) {
            try (ModelInput<MockData> in = WritableModelInput.open(f)) {
                results.putAll(MockData.collect(in));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
        return results;
    }

    private void flat(Action<DirectFileOutputPrepare, Exception> config, MockData... values) {
        VertexProcessorRunner runner = new VertexProcessorRunner(() -> {
            DirectFileOutputPrepare p = new DirectFileOutputPrepare();
            config.perform(p);
            return p;
        });
        runner
            .input(DirectFileOutputPrepare.INPUT_NAME, (Object[]) values)
            .resource(Configuration.class, configuration)
            .resource(StageInfo.class, STAGE)
            .run();

        commit();
    }

    private void group(Action<DirectFileOutputPrepare, Exception> config, MockData... inputs) {
        VertexProcessorRunner runner = new VertexProcessorRunner(() -> {
            DirectFileOutputPrepare p = new DirectFileOutputPrepare();
            config.perform(p);
            return p;
        });
        for (MockData d : inputs) {
            runner.group(DirectFileOutputPrepare.INPUT_NAME, d.getKey() + ".bin", d);
        }
        runner
            .resource(Configuration.class, configuration)
            .resource(StageInfo.class, STAGE)
            .run();

        commit();
    }

    private void commit() {
        VertexProcessorRunner committer = new VertexProcessorRunner(() -> {
            DirectFileOutputCommit p = new DirectFileOutputCommit();
            p.bind("a", "out");
            return p;
        });
        committer
            .resource(Configuration.class, configuration)
            .resource(StageInfo.class, STAGE)
            .run();
    }
}
