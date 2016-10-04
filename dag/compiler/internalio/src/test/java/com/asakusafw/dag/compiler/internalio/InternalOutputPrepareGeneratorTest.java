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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.testing.VertexProcessorRunner;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorTestRoot;
import com.asakusafw.dag.compiler.internalio.InternalOutputPrepareGenerator.Spec;
import com.asakusafw.dag.runtime.internalio.InternalOutputPrepare;
import com.asakusafw.dag.runtime.internalio.LocalInternalInputTaskInfo;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link InternalOutputPrepareGenerator}.
 */
public class InternalOutputPrepareGeneratorTest extends ClassGeneratorTestRoot {

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
    public void simple() throws Exception {
        File folder = temporary.newFolder();
        String pattern = folder.toURI().toString() + "/part-*";
        perform(pattern, "Hello, world!");
        assertThat(collect(folder), containsInAnyOrder("Hello, world!"));
    }

    private void perform(String pattern, String... values) {
        List<Spec> specs = Arrays.asList(new Spec("testing", pattern, Descriptions.typeOf(Text.class)));
        ClassGeneratorContext gc = context();
        ClassDescription gen = add(c -> new InternalOutputPrepareGenerator().generate(gc, specs, c));
        loading(gen, c -> {
            VertexProcessorRunner runner = new VertexProcessorRunner(() -> (VertexProcessor) c.newInstance());
            runner
                .input(InternalOutputPrepare.INPUT_NAME, Stream.of(values).map(Text::new).toArray())
                .resource(Configuration.class, new Configuration())
                .resource(StageInfo.class, STAGE)
                .run();
        });
    }

    private static List<String> collect(File folder) throws IOException {
        List<String> results = new ArrayList<>();
        for (File file : folder.listFiles(f -> f.getName().startsWith("part-"))) {
            try (ModelInput<Text> in = LocalInternalInputTaskInfo.open(file)) {
                Text buf = new Text();
                while (in.readTo(buf)) {
                    results.add(buf.toString());
                }
            }
        }
        return results;
    }
}
