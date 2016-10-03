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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
import com.asakusafw.dag.compiler.directio.DirectFileOutputCommitGenerator.Spec;
import com.asakusafw.dag.runtime.directio.BasicDataDefinition;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockDataFormat;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.directio.Counter;
import com.asakusafw.runtime.directio.DataDefinition;
import com.asakusafw.runtime.directio.DirectDataSource;
import com.asakusafw.runtime.directio.DirectDataSourceRepository;
import com.asakusafw.runtime.directio.OutputAttemptContext;
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link DirectFileOutputCommitGenerator}.
 */
public class DirectFileOutputCommitGeneratorTest extends ClassGeneratorTestRoot {

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

    private final List<Spec> specs = new ArrayList<>();

    private DirectDataSourceRepository repository;

    private Configuration configuration;

    /**
     * Set up.
     */
    @Before
    public void setup() {
        configuration = directio.newConfiguration();
        repository = HadoopDataSourceUtil.loadRepository(configuration);
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        File file = prepare("s", "out/testing.bin", "out", "testing.bin");
        assertThat(file.exists(), is(false));

        perform();
        assertThat(file.exists(), is(true));
    }

    private File prepare(
            String id, String physicalPath,
            String basePath, String resourceName) throws IOException, InterruptedException {
        String sourceId = repository.getRelatedId(basePath);
        String container = repository.getContainerPath(basePath);
        String component = repository.getComponentPath(basePath);
        DirectDataSource dataSource = repository.getRelatedDataSource(container);

        OutputAttemptContext context = new OutputAttemptContext(
                STAGE.getExecutionId(), "1",
                sourceId, new Counter());
        dataSource.setupAttemptOutput(context);
        DataDefinition<MockData> def = BasicDataDefinition.newInstance(new MockDataFormat());
        try (ModelOutput<MockData> out = dataSource.openOutput(context, def, component, resourceName, new Counter())) {
            out.write(new MockData().set(0, "Hello!"));
        }
        dataSource.commitAttemptOutput(context);
        dataSource.cleanupAttemptOutput(context);
        specs.add(new Spec(id, basePath));
        return directio.file(physicalPath);
    }

    private void perform() {
        ClassGeneratorContext gc = context();
        ClassDescription gen = add(c -> new DirectFileOutputCommitGenerator().generate(gc, specs, c));
        loading(gen, c -> {
            VertexProcessorRunner runner = new VertexProcessorRunner(() -> (VertexProcessor) c.newInstance());
            runner
                .resource(Configuration.class, configuration)
                .resource(StageInfo.class, STAGE)
                .run();
        });
    }
}
