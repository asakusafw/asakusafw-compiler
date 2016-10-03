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
package com.asakusafw.dag.runtime.skeleton;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.junit.Test;

import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.testing.VertexProcessorRunner;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.DataTableAdapter;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.dag.runtime.adapter.Operation;
import com.asakusafw.dag.runtime.adapter.OperationAdapter;
import com.asakusafw.dag.runtime.table.BasicDataTable;
import com.asakusafw.runtime.core.Result;

/**
 * Test for {@link VertexAdapter}.
 */
public class VertexAdapterTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        VertexProcessorRunner runner = new VertexProcessorRunner(() -> new VertexAdapter()
                .input(MockInput.class)
                .dataTable(MockTable.class)
                .operation(MockOperation.class)
                .output(MockOutput.class));
        runner.input("in", "Hello, input!");
        runner.output("out", Function.identity());
        runner.run();

        assertThat(runner.get("out"), containsInAnyOrder("Hello, input!!"));
    }

    @SuppressWarnings("javadoc")
    public static class MockInput extends ExtractInputAdapter {
        public MockInput(VertexProcessorContext context) {
            super(context);
            bind("in");
        }
    }

    @SuppressWarnings("javadoc")
    public static class MockOutput extends EdgeOutputAdapter {
        public MockOutput(VertexProcessorContext context) {
            super(context);
            bind("out");
        }
    }

    @SuppressWarnings("javadoc")
    public static class MockTable implements DataTableAdapter {
        public MockTable(VertexProcessorContext context) {
            return;
        }
        @Override
        public Set<String> getIds() {
            return Collections.singleton("table");
        }
        @SuppressWarnings("unchecked")
        @Override
        public <T> DataTable<T> getDataTable(Class<T> type, String id) {
            DataTable.Builder<String> builder = new BasicDataTable.Builder<>();
            builder.add(builder.newKeyBuffer(), "Hello, table!");
            return (DataTable<T>) builder.build();
        }
    }

    @SuppressWarnings("javadoc")
    public static class MockOperation implements OperationAdapter<ExtractOperation.Input> {
        public MockOperation(VertexProcessorContext context) {
            return;
        }
        @Override
        public Operation<? super ExtractOperation.Input> newInstance(OperationAdapter.Context context) {
            DataTable<String> table = context.getDataTable(String.class, "table");
            assertThat(table.getList(table.newKeyBuffer()), contains("Hello, table!"));
            Result<String> sink = context.getSink(String.class, "out");
            return input -> {
                String in = input.getObject();
                sink.add(in + "!");
            };
        }
    }
}
