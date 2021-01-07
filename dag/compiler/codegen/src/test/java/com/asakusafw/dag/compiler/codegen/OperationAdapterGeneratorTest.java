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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.compiler.model.graph.InputNode;
import com.asakusafw.dag.compiler.model.graph.OperationSpec;
import com.asakusafw.dag.compiler.model.graph.OutputNode;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.Operation;
import com.asakusafw.dag.runtime.adapter.OperationAdapter;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.testing.MockResult;

/**
 * Test for {@link OperationAdapterGenerator}.
 */
public class OperationAdapterGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        InputNode root = new InputNode(new OutputNode("testing", typeOf(Result.class), typeOf(String.class)));
        MockContext c = new MockContext();
        check(root, a -> {
            Operation<? super String> op = a.newInstance(c);
            op.process("Hello, world!");
        });
        assertThat(c.get("testing"), contains("Hello, world!"));
    }

    @SuppressWarnings("unchecked")
    private void check(InputNode root, Action<OperationAdapter<String>, ?> adapter) {
        ClassDescription generated = add(c -> new OperationAdapterGenerator().generate(context(), new OperationSpec(root), c));
        loading(generated, c -> {
            adapter.perform((OperationAdapter<String>) adapter(c, new MockVertexProcessorContext()));
        });
    }

    private static class MockContext implements OperationAdapter.Context {

        private final Map<String, MockResult<?>> results = new HashMap<>();

        MockContext() {
            return;
        }

        @SuppressWarnings("unchecked")
        <T> List<T> get(String id) {
            return (List<T>) Optionals.get(results, id)
                    .map(r -> r.getResults())
                    .orElseThrow(AssertionError::new);
        }

        @Override
        public ClassLoader getClassLoader() {
            return getClass().getClassLoader();
        }

        @Override
        public <T> DataTable<T> getDataTable(Class<T> type, String id) {
            throw new AssertionError();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> Result<T> getSink(Class<T> type, String id) {
            return (Result<T>) results.computeIfAbsent(id, v -> new MockResult<>());
        }

        @Override
        public ProcessorContext getDetached() {
            throw new UnsupportedOperationException();
        }
    }
}
