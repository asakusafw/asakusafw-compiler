/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.compiler.model.graph.DataTableNode;
import com.asakusafw.dag.compiler.model.graph.InputNode;
import com.asakusafw.dag.compiler.model.graph.OperationSpec;
import com.asakusafw.dag.compiler.model.graph.OperatorNode;
import com.asakusafw.dag.compiler.model.graph.OutputNode;
import com.asakusafw.dag.compiler.model.graph.SpecialElement;
import com.asakusafw.dag.compiler.model.graph.ValueElement;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.Operation;
import com.asakusafw.dag.runtime.adapter.OperationAdapter;
import com.asakusafw.dag.runtime.table.BasicDataTable;
import com.asakusafw.lang.compiler.model.description.BasicTypeDescription.BasicTypeKind;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.testing.MockResult;

/**
 * Test for {@link OperationGenerator}.
 */
public class OperationGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        InputNode root = new InputNode(new OutputNode("testing", typeOf(Result.class), typeOf(String.class)));
        MockContext context = new MockContext();
        testing(root, context, op -> {
            op.process("Hello, world!");
        });
        assertThat(context.get("testing"), contains("Hello, world!"));
    }

    /**
     * w/ operator.
     */
    @Test
    public void operator() {
        OutputNode output = new OutputNode("testing", typeOf(Result.class), typeOf(String.class));
        OperatorNode operator = new OperatorNode(
                classOf(SimpleOp.class), typeOf(Result.class), typeOf(String.class),
                output);
        InputNode root = new InputNode(operator);
        MockContext context = new MockContext();
        testing(root, context, op -> {
            op.process("Hello, world!");
        });
        assertThat(context.get("testing"), contains("Hello, world!?"));
    }

    /**
     * w/ operator + literal.
     */
    @Test
    public void operator_literal() {
        OutputNode output = new OutputNode("testing", typeOf(Result.class), typeOf(String.class));
        OperatorNode operator = new OperatorNode(
                classOf(SimpleOp.class), typeOf(Result.class), typeOf(String.class),
                output, new ValueElement(valueOf("???")));
        InputNode root = new InputNode(operator);
        MockContext context = new MockContext();
        testing(root, context, op -> {
            op.process("Hello, world!");
        });
        assertThat(context.get("testing"), contains("Hello, world!???"));
    }

    /**
     * w/ operator + class.
     */
    @Test
    public void operator_class() {
        OutputNode output = new OutputNode("testing", typeOf(Result.class), typeOf(String.class));
        OperatorNode operator = new OperatorNode(
                classOf(SimpleOp.class), typeOf(Result.class), typeOf(String.class),
                output, new ValueElement(valueOf(Integer.class)));
        InputNode root = new InputNode(operator);
        MockContext context = new MockContext();
        testing(root, context, op -> {
            op.process("Hello, world!");
        });
        assertThat(context.get("testing"), contains("Hello, world!java.lang.Integer"));
    }

    /**
     * w/ operator + enum.
     */
    @Test
    public void operator_enum() {
        OutputNode output = new OutputNode("testing", typeOf(Result.class), typeOf(String.class));
        OperatorNode operator = new OperatorNode(
                classOf(SimpleOp.class), typeOf(Result.class), typeOf(String.class),
                output, new ValueElement(valueOf(BasicTypeKind.VOID), typeOf(Enum.class)));
        InputNode root = new InputNode(operator);
        MockContext context = new MockContext();
        testing(root, context, op -> {
            op.process("Hello, world!");
        });
        assertThat(context.get("testing"), contains("Hello, world!VOID"));
    }

    /**
     * w/ operator + datatable.
     */
    @Test
    public void operator_table() {
        OutputNode output = new OutputNode("testing", typeOf(Result.class), typeOf(String.class));
        OperatorNode operator = new OperatorNode(
                classOf(SimpleOp.class), typeOf(Result.class), typeOf(String.class),
                output, new DataTableNode("t", typeOf(DataTable.class), typeOf(String.class)));
        InputNode root = new InputNode(operator);
        MockContext context = new MockContext();
        context.put("t", new BasicDataTable.Builder<>().build());
        testing(root, context, op -> {
            op.process("Hello, world!");
        });
        assertThat(context.get("testing"), contains("Hello, world!TABLE"));
    }

    /**
     * w/ operator + context.
     */
    @Test
    public void operator_context() {
        OutputNode output = new OutputNode("testing", typeOf(Result.class), typeOf(String.class));
        OperatorNode operator = new OperatorNode(
                classOf(SimpleOp.class), typeOf(Result.class), typeOf(String.class),
                output, new SpecialElement(VertexElement.ElementKind.CONTEXT, typeOf(ProcessorContext.class)));
        InputNode root = new InputNode(operator);
        MockContext context = new MockContext();
        testing(root, context, op -> {
            op.process("Hello, world!");
        });
        assertThat(context.get("testing"), contains("Hello, world!CONTEXT"));
    }

    /**
     * w/ operator + context.
     */
    @Test
    public void operator_empty_data_table() {
        OutputNode output = new OutputNode("testing", typeOf(Result.class), typeOf(String.class));
        OperatorNode operator = new OperatorNode(
                classOf(SimpleOp.class), typeOf(Result.class), typeOf(String.class),
                output, new SpecialElement(VertexElement.ElementKind.EMPTY_DATA_TABLE, typeOf(DataTable.class)));
        InputNode root = new InputNode(operator);
        MockContext context = new MockContext();
        testing(root, context, op -> {
            op.process("Hello, world!");
        });
        assertThat(context.get("testing"), contains("Hello, world!TABLE"));
    }

    private void testing(InputNode node, OperationAdapter.Context context, Action<Operation<Object>, ?> action) {
        ClassDescription aClass = add(c -> new OperationGenerator().generate(context(), new OperationSpec(node), c));
        loading(cl -> {
            Constructor<?> ctor = aClass.resolve(cl).getConstructor(OperationAdapter.Context.class);
            @SuppressWarnings("unchecked")
            Operation<Object> op = (Operation<Object>) ctor.newInstance(context);
            action.perform(op);
        });
    }

    private static class MockContext implements OperationAdapter.Context {

        private final Map<String, MockResult<?>> results = new HashMap<>();

        private final Map<String, DataTable<?>> tables = new HashMap<>();

        MockContext() {
            return;
        }

        @SuppressWarnings("unchecked")
        <T> List<T> get(String id) {
            return (List<T>) Optionals.get(results, id)
                    .map(r -> r.getResults())
                    .orElseThrow(AssertionError::new);
        }

        void put(String id, DataTable<?> table) {
            tables.put(id, table);
        }

        @Override
        public ClassLoader getClassLoader() {
            return getClass().getClassLoader();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> DataTable<T> getDataTable(Class<T> type, String id) {
            return (DataTable<T>) Invariants.requireNonNull(tables.get(id));
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

    @SuppressWarnings("javadoc")
    public static class SimpleOp implements Result<String> {

        private final Result<String> chain;

        private final String suffix;

        public SimpleOp(Result<String> chain) {
            this(chain, "?");
        }

        public SimpleOp(Result<String> chain, String suffix) {
            this.chain = chain;
            this.suffix = suffix;
        }

        public SimpleOp(Result<String> chain, Class<?> suffix) {
            this.chain = chain;
            this.suffix = suffix.getName();
        }

        public SimpleOp(Result<String> chain, Enum<?> suffix) {
            this.chain = chain;
            this.suffix = suffix.name();
        }

        public SimpleOp(Result<String> chain, DataTable<?> suffix) {
            assertThat(suffix, is(notNullValue()));
            this.chain = chain;
            this.suffix = "TABLE";
        }

        public SimpleOp(Result<String> chain, ProcessorContext suffix) {
            assertThat(suffix, is(notNullValue()));
            this.chain = chain;
            this.suffix = "CONTEXT";
        }

        @Override
        public void add(String result) {
            chain.add(result + suffix);
        }
    }
}
