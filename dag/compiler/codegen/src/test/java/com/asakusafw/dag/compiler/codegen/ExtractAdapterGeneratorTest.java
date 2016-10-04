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

import org.junit.Test;

import com.asakusafw.dag.compiler.model.graph.OutputNode;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.testing.MockResult;

/**
 * Test for {@link ExtractAdapterGenerator}.
 */
public class ExtractAdapterGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockResult<String> r = new MockResult<>();
        add(r, "Hello, world!");
        assertThat(r.getResults(), contains("Hello, world!"));
    }

    private void add(Result<?> result, Object... values) {
        VertexElement element = new OutputNode("t", typeOf(Result.class), typeOf(String.class));
        ClassDescription generated = add(c -> new ExtractAdapterGenerator().generate(element, c));
        loading(generated, c -> {
            @SuppressWarnings("unchecked")
            Result<Object> r = (Result<Object>) c.getConstructor(Result.class).newInstance(result);
            for (Object value : values) {
                r.add(new Mock(value));
            }
        });
    }

    private static class Mock implements ExtractOperation.Input {

        private final Object object;

        public Mock(Object object) {
            this.object = object;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getObject() {
            return (T) object;
        }
    }
}
