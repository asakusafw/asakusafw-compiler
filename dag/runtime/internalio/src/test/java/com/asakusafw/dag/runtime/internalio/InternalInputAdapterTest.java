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
package com.asakusafw.dag.runtime.internalio;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.testing.MockTaskProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.ExtractOperation.Input;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.InputHandler.InputSession;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link InternalInputAdapter}.
 */
public class InternalInputAdapterTest {

    /**
     * Support for Windows platform.
     */
    @ClassRule
    public static final WindowsSupport WINDOWS_SUPPORT = new WindowsSupport();

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void test() throws Exception {
        File folder = temporary.newFolder();
        File f = put(new File(folder, "a.bin"), "Hello, world!");
        MockVertexProcessorContext vc = new MockVertexProcessorContext()
                .withResource(StageInfo.class, STAGE)
                .withResource(Configuration.class, new Configuration());
        List<String> results;
        try (InternalInputAdapter adapter = new InternalInputAdapter(vc)) {
            adapter.bind("testing", f.toURI().toString(), Text.class);
            adapter.initialize();
            results = collect(adapter);
        }
        assertThat(results, containsInAnyOrder("Hello, world!"));
    }

    private static File put(File file, String... lines) throws IOException {
        Lang.let(file.getParentFile(), f -> Assume.assumeTrue(f.mkdirs() || f.isDirectory()));
        try (ModelOutput<Text> out = InternalOutputHandler.create(new FileOutputStream(file), Text.class)) {
            Text buf = new Text();
            for (String line : lines) {
                buf.set(line);
                out.write(buf);
            }
        }
        return file;
    }

    private static List<String> collect(InternalInputAdapter adapter) throws IOException, InterruptedException {
        List<String> results = new ArrayList<>();
        TaskSchedule schedule = adapter.getSchedule();
        InputHandler<Input, ? super TaskProcessorContext> handler = adapter.newHandler();
        for (TaskInfo info : schedule.getTasks()) {
            try (InputSession<Input> session = handler.start(new MockTaskProcessorContext(info))) {
                while (session.next()) {
                    Input input = session.get();
                    Text object = input.getObject();
                    results.add(object.toString());
                }
            }
        }
        return results;
    }
}
