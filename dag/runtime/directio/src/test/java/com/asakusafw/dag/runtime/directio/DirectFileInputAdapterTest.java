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
package com.asakusafw.dag.runtime.directio;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.testing.MockTaskProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.ExtractOperation.Input;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.InputHandler.InputSession;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockDataFormat;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.WritableModelOutput;
import com.asakusafw.lang.utils.common.AssertUtil;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link DirectFileInputAdapter}.
 */
public class DirectFileInputAdapterTest {

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

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        put("in/testing.bin", "Hello, world!");
        MockVertexProcessorContext vc = new MockVertexProcessorContext()
                .withResource(StageInfo.class, STAGE)
                .withResource(Configuration.class, directio.newConfiguration());
        try (DirectFileInputAdapter adapter = new DirectFileInputAdapter(vc)) {
            adapter.bind("testing", "in", "*.bin", MockDataFormat.class, null, false);
            adapter.initialize();
            assertThat(collect(adapter), containsInAnyOrder("Hello, world!"));
        }
    }

    /**
     * missing mandatory input.
     * @throws Exception if failed
     */
    @Test
    public void missing_mandatory() throws Exception {
        MockVertexProcessorContext vc = new MockVertexProcessorContext()
                .withResource(StageInfo.class, STAGE)
                .withResource(Configuration.class, directio.newConfiguration());
        AssertUtil.catching(() -> {
            try (DirectFileInputAdapter adapter = new DirectFileInputAdapter(vc)) {
                adapter.bind("testing", "in", "*.bin", MockDataFormat.class, null, false);
                adapter.initialize();
                collect(adapter);
            }
        });
    }

    /**
     * missing mandatory input.
     * @throws Exception if failed
     */
    @Test
    public void missing_optional() throws Exception {
        MockVertexProcessorContext vc = new MockVertexProcessorContext()
                .withResource(StageInfo.class, STAGE)
                .withResource(Configuration.class, directio.newConfiguration());
        try (DirectFileInputAdapter adapter = new DirectFileInputAdapter(vc)) {
            adapter.bind("testing", "in", "*.bin", MockDataFormat.class, null, true);
            adapter.initialize();
            assertThat(collect(adapter), is(empty()));
        }
    }

    private File put(String path, String... lines) throws IOException {
        File file = directio.file(path);
        try (ModelOutput<MockData> out = WritableModelOutput.create(file)) {
            MockData buf = new MockData();
            for (String line : lines) {
                out.write(buf.set(0, line));
            }
        }
        return file;
    }

    private static List<String> collect(DirectFileInputAdapter adapter) throws IOException, InterruptedException {
        List<String> results = new ArrayList<>();
        TaskSchedule schedule = adapter.getSchedule();
        InputHandler<Input, ? super TaskProcessorContext> handler = adapter.newHandler();
        for (TaskInfo info : schedule.getTasks()) {
            try (InputSession<Input> session = handler.start(new MockTaskProcessorContext(info))) {
                while (session.next()) {
                    Input input = session.get();
                    MockData object = input.getObject();
                    results.add(object.getValue());
                }
            }
        }
        return results;
    }
}
