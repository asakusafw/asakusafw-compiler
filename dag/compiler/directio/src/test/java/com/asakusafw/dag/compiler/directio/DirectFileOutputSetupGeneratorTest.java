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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.testing.VertexProcessorRunner;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorTestRoot;
import com.asakusafw.dag.compiler.directio.DirectFileOutputSetupGenerator.Spec;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Test for {@link DirectFileOutputSetupGenerator}.
 */
public class DirectFileOutputSetupGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * Direct I/O testing context.
     */
    @Rule
    public final DirectIoContext directio = new DirectIoContext();

    private final List<Spec> specs = new ArrayList<>();

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        File file = create("out/testing.bin");
        add("out", "*.bin");
        perform();
        assertThat(file.exists(), is(false));
    }

    /**
     * out of scope.
     * @throws Exception if failed
     */
    @Test
    public void out_of_scope() throws Exception {
        File file = create("out/testing.bin");
        add("out", "*.txt");
        perform();
        assertThat(file.exists(), is(true));
    }

    private File create(String path) throws IOException {
        File file = directio.file(path);
        File parent = file.getParentFile();
        assertThat(parent.mkdirs() || parent.isDirectory(), is(true));
        assertThat(file.createNewFile(), is(true));
        return file;
    }

    private void add(String basePath, String... patterns) {
        specs.add(new Spec("t", basePath, Arrays.asList(patterns)));
    }

    private void perform() {
        ClassGeneratorContext gc = context();
        ClassDescription gen = add(c -> new DirectFileOutputSetupGenerator().generate(gc, specs, c));
        loading(gen, c -> {
            VertexProcessorRunner runner = new VertexProcessorRunner(() -> (VertexProcessor) c.newInstance());
            runner
                .resource(Configuration.class, directio.newConfiguration())
                .resource(StageInfo.class, STAGE)
                .run();
        });
    }
}
