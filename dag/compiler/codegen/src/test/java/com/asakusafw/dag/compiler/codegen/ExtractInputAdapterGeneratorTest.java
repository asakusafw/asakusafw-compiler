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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.asakusafw.dag.api.processor.EdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.testing.CollectionObjectReader;
import com.asakusafw.dag.api.processor.testing.MockTaskProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.compiler.codegen.ExtractInputAdapterGenerator.Spec;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.InputHandler.InputSession;
import com.asakusafw.dag.runtime.skeleton.ExtractInputAdapter;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Test for {@link ExtractInputAdapterGenerator}.
 */
public class ExtractInputAdapterGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        check("Hello, world!");
    }

    /**
     * multiple inputs.
     */
    @Test
    public void multiple() {
        check("A", "B", "C");
    }

    private void check(String... values) {
        Spec spec = new Spec("i", typeOf(String.class));
        ClassGeneratorContext gc = context();
        ClassDescription generated = add(c -> new ExtractInputAdapterGenerator().generate(gc, spec, c));
        MockTaskProcessorContext tc = new MockTaskProcessorContext("t")
                .withInput("i", () -> new CollectionObjectReader(Arrays.asList(values)));

        List<Object> results = new ArrayList<>();
        loading(generated, c -> {
            try (ExtractInputAdapter adapter = adapter(c, new MockVertexProcessorContext().with(c))) {
                adapter.initialize();
                InputHandler<ExtractOperation.Input, ? super EdgeIoProcessorContext> handler = adapter.newHandler();
                try (InputSession<ExtractOperation.Input> session = handler.start(tc)) {
                    while (session.next()) {
                        results.add(session.get().getObject());
                    }
                }
            }
        });
        assertThat(results, containsInAnyOrder((Object[]) values));
    }
}
