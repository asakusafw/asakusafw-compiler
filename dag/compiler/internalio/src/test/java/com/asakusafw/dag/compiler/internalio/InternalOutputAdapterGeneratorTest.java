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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockTaskProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorTestRoot;
import com.asakusafw.dag.compiler.internalio.InternalOutputAdapterGenerator.Spec;
import com.asakusafw.dag.runtime.adapter.ContextHandler.Session;
import com.asakusafw.dag.runtime.adapter.OutputAdapter;
import com.asakusafw.dag.runtime.adapter.OutputHandler;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.runtime.util.hadoop.ConfigurationProvider;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link InternalInputAdapterGenerator}.
 */
public class InternalOutputAdapterGeneratorTest extends ClassGeneratorTestRoot {

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
        Path path = new Path(new File(temporary.getRoot(), "part-*").toURI());
        Configuration conf = configurations.newInstance();

        ClassGeneratorContext gc = context();
        Spec spec = new Spec("o", path.toString(), Descriptions.typeOf(Text.class));
        ClassDescription gen = add(c -> new InternalOutputAdapterGenerator().generate(gc, Arrays.asList(spec), c));
        loading(gen, c -> {
            VertexProcessorContext vc = new MockVertexProcessorContext()
                    .with(c)
                    .withResource(conf)
                    .withResource(new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap()));
            try (OutputAdapter adapter = adapter(c, vc)) {
                adapter.initialize();
                OutputHandler<? super TaskProcessorContext> handler = adapter.newHandler();
                Result<Text> sink = handler.getSink(Text.class, "o");
                try (Session session = handler.start(new MockTaskProcessorContext("t"))) {
                    for (String v : values) {
                        sink.add(new Text(v));
                    }
                }
            }
        });

        Set<String> results = new LinkedHashSet<>();
        try {
            List<Path> paths = TemporaryStorage.list(conf, path);
            Text buf = new Text();
            for (Path p : paths) {
                try (ModelInput<Text> in = TemporaryStorage.openInput(conf, Text.class, p)) {
                    while (in.readTo(buf)) {
                        results.add(buf.toString());
                    }
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        assertThat(results, containsInAnyOrder(values));
    }
}
