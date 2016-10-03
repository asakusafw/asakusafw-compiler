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
package com.asakusafw.dag.compiler.internalio;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockTaskProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorTestRoot;
import com.asakusafw.dag.compiler.internalio.InternalInputAdapterGenerator.Spec;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.InputHandler.InputSession;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.runtime.util.hadoop.ConfigurationProvider;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link InternalInputAdapterGenerator}.
 */
public class InternalInputAdapterGeneratorTest extends ClassGeneratorTestRoot {

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

    private final ConfigurationProvider configurations = new ConfigurationProvider();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        check("Hello, world!");
    }

    /**
     * multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        check("A", "B", "C");
    }

    private void check(String... values) {
        Path path = new Path(new File(temporary.getRoot(), "temp.bin").toURI());
        Configuration conf = configurations.newInstance();
        try (ModelOutput<Text> out = TemporaryStorage.openOutput(conf, Text.class, path)) {
            for (String v : values) {
                out.write(new Text(v));
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        ClassGeneratorContext gc = context();
        Spec spec = new Spec("testing", Collections.singleton(path.toString()), Descriptions.typeOf(Text.class));
        ClassDescription gen = add(c -> new InternalInputAdapterGenerator().generate(gc, spec, c));

        List<String> results = new ArrayList<>();
        loading(gen, c -> {
            VertexProcessorContext vc = new MockVertexProcessorContext()
                    .with(c)
                    .withResource(conf)
                    .withResource(new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap()));
            input(vc, c, o -> {
                results.add(o.toString());
            });
        });
        assertThat(results, containsInAnyOrder((Object[]) values));
    }

    private void input(VertexProcessorContext context, Class<?> c, Consumer<Object> action) {
        try (InputAdapter<?> adapter = adapter(c, context)) {
            adapter.initialize();
            TaskSchedule sched = adapter.getSchedule();
            assertThat(sched, is(notNullValue()));
            for (TaskInfo t : sched.getTasks()) {
                MockTaskProcessorContext tc = new MockTaskProcessorContext("t", t);
                @SuppressWarnings("unchecked")
                InputHandler<ExtractOperation.Input, ? super TaskProcessorContext> handler =
                (InputHandler<ExtractOperation.Input, ? super TaskProcessorContext>) adapter.newHandler();
                try (InputSession<ExtractOperation.Input> session = handler.start(tc)) {
                    while (session.next()) {
                        action.accept(session.get().getObject());
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
