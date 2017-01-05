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
package com.asakusafw.dag.compiler.builtin;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorTestRoot;
import com.asakusafw.dag.compiler.codegen.CompositeOperatorNodeGenerator;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.compiler.model.graph.DataTableNode;
import com.asakusafw.dag.compiler.model.graph.OutputNode;
import com.asakusafw.dag.compiler.model.graph.ValueElement;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.runtime.adapter.CoGroupOperation;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorArgument;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty.PropertyKind;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.Result;

/**
 * Test root for the operator node generators.
 */
public abstract class OperatorNodeGeneratorTestRoot extends ClassGeneratorTestRoot {

    /**
     * Returns a dummy {@link VertexElement}.
     * @param type the data type
     * @return the dummy
     */
    public VertexElement result(TypeDescription type) {
        return new OutputNode("testing", Descriptions.typeOf(Result.class), type);
    }

    /**
     * Returns a new generator context.
     * @param callback the callback
     * @return the context
     */
    public OperatorNodeGenerator.Context prepare(Consumer<Map<OperatorProperty, VertexElement>> callback) {
        Mock mock = new Mock(context());
        callback.accept(mock.dependencies);
        return mock;
    }

    /**
     * Performs operator node generator.
     * @param operator the target operator
     * @return the result
     */
    public NodeInfo generate(Operator operator) {
        return generate(operator, prepare(m -> {
            operator.getInputs().stream()
                .filter(p -> p.getInputUnit() == InputUnit.WHOLE)
                .forEach(p -> m.put(p, table(p)));
            operator.getOutputs().forEach(o -> m.put(o, result(o.getDataType())));
        }));
    }

    private VertexElement table(OperatorInput port) {
        return new DataTableNode(port.getName(), Descriptions.typeOf(DataTable.class), port.getDataType());
    }

    /**
     * Performs operator node generator.
     * @param operator the target operator
     * @param callback the callback
     * @return the result
     */
    public NodeInfo generate(Operator operator, Consumer<Map<OperatorProperty, VertexElement>> callback) {
        return generate(operator, prepare(callback));
    }

    /**
     * Performs operator node generator.
     * @param operator the target operator
     * @param context the current context
     * @return the result
     */
    public NodeInfo generate(Operator operator, OperatorNodeGenerator.Context context) {
        CompositeOperatorNodeGenerator generator = CompositeOperatorNodeGenerator.load(context.getClassLoader());
        NodeInfo info = generator.generate(context, operator);
        context.addClassFile(info.getClassData());
        return Invariants.requireNonNull(info, () -> operator);
    }

    /**
     * Loads {@link NodeInfo} and provides its constructor.
     * @param info the node info
     * @param action action for the loaded constructor
     */
    public void loading(NodeInfo info, Action<Constructor<? extends Result<Object>>, ?> action) {
        loading(cl -> {
            Class<?>[] types = info.getDependencies().stream()
                    .map(e -> Lang.safe(() -> e.getRuntimeType().getErasure().resolve(cl)))
                    .toArray(Class<?>[]::new);
            @SuppressWarnings("unchecked")
            Constructor<? extends Result<Object>> ctor = (Constructor<? extends Result<Object>>) info.getClassData()
                    .getDescription().resolve(cl)
                    .getConstructor(types);
            action.perform(ctor);
        });
    }

    /**
     * Returns a new input object of {@link CoGroupOperation}.
     * @param groups the elements
     * @return the created input
     */
    public static CoGroupOperation.Input cogroup(Object[][] groups) {
        return new CoGroupOperation.Input() {
            @Override
            public <T> CoGroupOperation.Cursor<T> getCursor(int index) throws IOException, InterruptedException {
                Object[] group = groups[index];
                return new CoGroupOperation.Cursor<T>() {
                    private int cursor = -1;
                    @Override
                    public boolean nextObject() throws IOException, InterruptedException {
                        if (cursor + 1 < group.length) {
                            cursor++;
                            return true;
                        }
                        return false;
                    }
                    @SuppressWarnings("unchecked")
                    @Override
                    public T getObject() throws IOException, InterruptedException {
                        return (T) group[cursor];
                    }
                };
            }
            @SuppressWarnings("unchecked")
            @Override
            public <T> List<T> getList(int index) throws IOException, InterruptedException {
                return (List<T>) Arrays.asList(groups[index]);
            }
        };
    }

    private static class Mock implements OperatorNodeGenerator.Context, ClassGeneratorContext.Forward {

        private final ClassGeneratorContext forward;

        final Map<OperatorProperty, VertexElement> dependencies = new HashMap<>();

        public Mock(ClassGeneratorContext forward) {
            this.forward = forward;
        }

        @Override
        public ClassGeneratorContext getForward() {
            return forward;
        }

        @Override
        public VertexElement getDependency(OperatorProperty property) {
            if (property.getPropertyKind() == PropertyKind.ARGUMENT) {
                return dependencies.computeIfAbsent(property, p -> {
                    return new ValueElement(((OperatorArgument) property).getValue());
                });
            }
            return Invariants.requireNonNull(dependencies.get(property));
        }
    }
}
