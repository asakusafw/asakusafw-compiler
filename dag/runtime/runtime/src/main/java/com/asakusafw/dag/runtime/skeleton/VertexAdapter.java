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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.ForwardProcessorContext;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.DataTableAdapter;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.Operation;
import com.asakusafw.dag.runtime.adapter.OperationAdapter;
import com.asakusafw.dag.runtime.adapter.OutputAdapter;
import com.asakusafw.dag.runtime.adapter.OutputHandler;
import com.asakusafw.dag.runtime.adapter.VertexElementAdapter;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.runtime.core.Result;

/**
 * An adapter implementation of {@link VertexProcessor}s.
 * @since 0.4.0
 * @see #input(Class)
 * @see #dataTable(Class)
 * @see #operation(Class)
 * @see #output(Class)
 */
public class VertexAdapter implements VertexProcessor {

    private final Closer closer = new Closer();

    private final AtomicReference<Function<VertexProcessorContext, ? extends InputAdapter<?>>>
            inputAdapterProvider = new AtomicReference<>();

    private final List<Function<VertexProcessorContext, ? extends DataTableAdapter>>
            dataTableAdapterProviders = Collections.synchronizedList(new ArrayList<>());

    private final AtomicReference<Function<VertexProcessorContext, ? extends OperationAdapter<?>>>
            operationAdapterProvider = new AtomicReference<>();

    private final List<Function<VertexProcessorContext, ? extends OutputAdapter>>
            outputAdapterProviders = Collections.synchronizedList(new ArrayList<>());

    private final AtomicReference<ProcessorContext> rootContext = new AtomicReference<>();

    private final AtomicReference<InputAdapter<?>> inputAdapter = new AtomicReference<>();

    private final List<DataTableAdapter> dataTableAdapters = Collections.synchronizedList(new ArrayList<>());

    private final AtomicReference<OperationAdapter<?>> operationAdapter = new AtomicReference<>();

    private final List<OutputAdapter> outputAdapters = Collections.synchronizedList(new ArrayList<>());

    /**
     * Sets an {@link InputAdapter} class for the vertex input.
     * @param adapter the adapter class
     * @return this
     */
    public final VertexAdapter input(Class<? extends InputAdapter<?>> adapter) {
        Arguments.requireNonNull(adapter);
        return input(resolve(adapter));
    }

    /**
     * Sets an {@link InputAdapter} for the vertex input.
     * @param adapter the adapter class
     * @return this
     */
    public final VertexAdapter input(Function<VertexProcessorContext, ? extends InputAdapter<?>> adapter) {
        Arguments.requireNonNull(adapter);
        if (inputAdapterProvider.compareAndSet(null, adapter) == false) {
            throw new IllegalStateException();
        }
        return this;
    }

    /**
     * Adds a {@link DataTableAdapter} class for the vertex input.
     * @param adapter the adapter class
     * @return this
     */
    public final VertexAdapter dataTable(Class<? extends DataTableAdapter> adapter) {
        Arguments.requireNonNull(adapter);
        return dataTable(resolve(adapter));
    }

    /**
     * Adds a {@link DataTableAdapter} class for the vertex input.
     * @param adapter the adapter class
     * @return this
     */
    public final VertexAdapter dataTable(Function<VertexProcessorContext, ? extends DataTableAdapter> adapter) {
        Arguments.requireNonNull(adapter);
        dataTableAdapterProviders.add(adapter);
        return this;
    }

    /**
     * Sets an {@link OperationAdapter} class for the vertex input.
     * @param adapter the adapter class
     * @return this
     */
    public final VertexAdapter operation(Class<? extends OperationAdapter<?>> adapter) {
        Arguments.requireNonNull(adapter);
        return operation(resolve(adapter));
    }

    /**
     * Sets an {@link OperationAdapter} class for the vertex input.
     * @param adapter the adapter class
     * @return this
     */
    public final VertexAdapter operation(Function<VertexProcessorContext, ? extends OperationAdapter<?>> adapter) {
        Arguments.requireNonNull(adapter);
        if (operationAdapterProvider.compareAndSet(null, adapter) == false) {
            throw new IllegalStateException();
        }
        return this;
    }

    /**
     * Adds an {@link OutputAdapter} class for the vertex input.
     * @param adapter the adapter class
     * @return this
     */
    public final VertexAdapter output(Class<? extends OutputAdapter> adapter) {
        Arguments.requireNonNull(adapter);
        return output(resolve(adapter));
    }

    /**
     * Adds an {@link OutputAdapter} class for the vertex input.
     * @param adapter the adapter class
     * @return this
     */
    public final VertexAdapter output(Function<VertexProcessorContext, ? extends OutputAdapter> adapter) {
        Arguments.requireNonNull(adapter);
        outputAdapterProviders.add(adapter);
        return this;
    }

    private static <T extends VertexElementAdapter> Function<VertexProcessorContext, T> resolve(
            Class<T> adapterClass) {
        try {
            Constructor<T> constructor = adapterClass.getConstructor(VertexProcessorContext.class);
            return v -> {
                try {
                    return constructor.newInstance(v);
                } catch (Exception e) {
                    throw new IllegalStateException(MessageFormat.format(
                            "error occurred while initilizing adapter: {0}",
                            adapterClass.getName()), e);
                }
            };
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "adapter {0} does not have a constructor with {1}",
                    adapterClass.getName(),
                    VertexProcessorContext.class.getSimpleName()), e);
        }
    }

    @Override
    public Optional<? extends TaskSchedule> initialize(
            VertexProcessorContext context) throws IOException, InterruptedException {
        initializeAdapters(context);
        TaskSchedule schedule = inputAdapter.get().getSchedule();
        return Optionals.of(schedule);
    }

    private void initializeAdapters(VertexProcessorContext context) throws IOException, InterruptedException {
        try (Initializer<Closer> initializer = new Initializer<>(closer)) {
            rootContext.set(context.getDetached());
            closer.add(() -> rootContext.set(null));
            doInitialize(inputAdapterProvider, inputAdapter::set, context);
            doInitialize(operationAdapterProvider, operationAdapter::set, context);
            doInitialize(dataTableAdapterProviders, dataTableAdapters::add, context);
            doInitialize(outputAdapterProviders, outputAdapters::add, context);
            initializer.done();
        }
    }

    private <T extends VertexElementAdapter> void doInitialize(
            AtomicReference<Function<VertexProcessorContext, ? extends T>> factory,
            Consumer<? super T> target,
            VertexProcessorContext context) throws IOException, InterruptedException {
        Function<VertexProcessorContext, ? extends T> f = Invariants.requireNonNull(factory.get());
        T adapter = closer.add(f.apply(context));
        target.accept(adapter);
        adapter.initialize();
    }

    private <T extends VertexElementAdapter> void doInitialize(
            List<Function<VertexProcessorContext, ? extends T>> factories,
            Consumer<? super T> target,
            VertexProcessorContext context) throws IOException, InterruptedException {
        for (Function<VertexProcessorContext, ? extends T> factory : factories) {
            T adapter = closer.add(factory.apply(context));
            target.accept(adapter);
            adapter.initialize();
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
        List<OutputHandler<? super TaskProcessorContext>> outputs = new ArrayList<>();
        for (OutputAdapter adapter : outputAdapters) {
            outputs.add(adapter.newHandler());
        }
        InputHandler input = inputAdapter.get().newHandler();
        Operation operation = operationAdapter.get().newInstance(new OperationContext(
                Invariants.requireNonNull(rootContext.get()),
                outputs,
                dataTableAdapters));
        return new GenericTaskProcessor<>(input, operation, outputs);
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closer.close();
    }

    private static class OperationContext implements OperationAdapter.Context, ForwardProcessorContext {

        private final ProcessorContext forward;

        private final List<? extends OutputHandler<?>> outputs;

        private final List<? extends DataTableAdapter> dataTables;

        OperationContext(
                ProcessorContext forward,
                List<? extends OutputHandler<?>> outputs,
                List<? extends DataTableAdapter> dataTables) {
            this.forward = forward;
            this.outputs = outputs;
            this.dataTables = dataTables;
        }

        @Override
        public ProcessorContext getForward() {
            return forward;
        }

        @Override
        public <T> DataTable<T> getDataTable(Class<T> type, String id) {
            for (DataTableAdapter adapter : dataTables) {
                if (adapter.getIds().contains(id)) {
                    return adapter.getDataTable(type, id);
                }
            }
            throw new IllegalStateException(id);
        }

        @Override
        public <T> Result<T> getSink(Class<T> type, String id) {
            for (OutputHandler<?> handler : outputs) {
                if (handler.contains(id)) {
                    return handler.getSink(type, id);
                }
            }
            throw new IllegalStateException(id);
        }
    }
}
