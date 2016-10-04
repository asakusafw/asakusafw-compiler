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
package com.asakusafw.dag.compiler.directio;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.testing.VertexProcessorRunner;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorTestRoot;
import com.asakusafw.dag.compiler.directio.DirectFileOutputPrepareGenerator.Spec;
import com.asakusafw.dag.runtime.directio.DirectFileOutputCommit;
import com.asakusafw.dag.runtime.directio.DirectFileOutputPrepare;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockDataFormat;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableModelInput;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link DirectFileOutputPrepareGenerator}.
 */
public class DirectFileOutputPrepareGeneratorTest extends ClassGeneratorTestRoot {

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
        flat("a", "out", "*.bin", MockDataFormat.class,
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
        group("a", "out", MockDataFormat.class,
                new MockData().set(100, "Hello, world!"));
        Map<Integer, String> results = collect("out/100.bin");
        assertThat(results.keySet(), hasSize(1));
        assertThat(results, hasEntry(100, "Hello, world!"));
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

    private void flat(String id, String basePath, String outputPattern, Class<?> formatType, MockData... values) {
        List<Spec> specs = Arrays.asList(new Spec(id, basePath, outputPattern, Descriptions.typeOf(formatType)));
        ClassGeneratorContext gc = context();
        ClassDescription gen = add(c -> new DirectFileOutputPrepareGenerator().generate(gc, specs, c));
        loading(gen, c -> {
            VertexProcessorRunner runner = new VertexProcessorRunner(() -> (VertexProcessor) c.newInstance());
            runner
                .input(DirectFileOutputPrepare.INPUT_NAME, (Object[]) values)
                .resource(Configuration.class, configuration)
                .resource(StageInfo.class, STAGE)
                .run();
        });
        commit();
    }

    private void group(String id, String basePath, Class<?> formatType, MockData... inputs) {
        List<Spec> specs = Arrays.asList(new Spec(id, basePath, null, Descriptions.typeOf(formatType)));
        ClassGeneratorContext gc = context();
        ClassDescription gen = add(c -> new DirectFileOutputPrepareGenerator().generate(gc, specs, c));
        loading(gen, c -> {
            VertexProcessorRunner runner = new VertexProcessorRunner(() -> (VertexProcessor) c.newInstance());
            for (MockData d : inputs) {
                runner.group(DirectFileOutputPrepare.INPUT_NAME, d.getKey() + ".bin", d);
            }
            runner
                .resource(Configuration.class, configuration)
                .resource(StageInfo.class, STAGE)
                .run();
        });
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
