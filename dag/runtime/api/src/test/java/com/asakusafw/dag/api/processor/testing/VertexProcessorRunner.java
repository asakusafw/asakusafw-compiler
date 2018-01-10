/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.api.processor.testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.basic.AbstractEdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.basic.AbstractProcessorContext;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;

/**
 * Test runner for {@link VertexProcessor}.
 */
public class VertexProcessorRunner implements Runnable {

    private final Callable<? extends VertexProcessor> processorFactory;

    private final Map<Class<?>, Supplier<?>> resources = new LinkedHashMap<>();

    private final Map<String, List<Object>> mainInputs = new LinkedHashMap<>();

    private final Map<String, SortedMap<Object, Collection<Object>>> groupInputs = new LinkedHashMap<>();

    private final Map<String, List<Object>> subInputs = new LinkedHashMap<>();

    private int inputSplit;

    private final Map<String, CollectionObjectWriter> outputs = new LinkedHashMap<>();

    /**
     * Creates a new instance.
     * @param processor the target processor
     */
    public VertexProcessorRunner(Callable<? extends VertexProcessor> processor) {
        this.processorFactory = processor;
    }

    @Override
    public void run() {
        try (VertexProcessor processor = processorFactory.call()) {
            Optional<List<? extends TaskInfo>> tasks = doInitialize(processor);
            if (tasks.isPresent()) {
                Invariants.require(mainInputs.isEmpty());
                Invariants.require(groupInputs.isEmpty());
                doCustomTasks(processor, tasks.get());
            } else if (mainInputs.isEmpty() == false) {
                Invariants.require(mainInputs.size() == 1);
                Invariants.require(groupInputs.isEmpty());
                doFlatTasks(processor);
            } else {
                Invariants.require(mainInputs.isEmpty());
                Invariants.require(groupInputs.size() >= 1);
                doGroupTasks(processor);
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private Optional<List<? extends TaskInfo>> doInitialize(
            VertexProcessor processor) throws IOException, InterruptedException {
        MockVertexProcessorContext context = new MockVertexProcessorContext();
        putResources(context);
        subInputs.forEach((n, l) -> {
            context.withInput(n, () -> new CollectionObjectReader(l));
        });
        putOutputs(context);
        Optional<? extends TaskSchedule> sched = processor.initialize(context);
        return sched.map(s -> Lang.safe(() -> s.getTasks()));
    }

    private void doCustomTasks(
            VertexProcessor processor,
            List<? extends TaskInfo> tasks) throws IOException, InterruptedException {
        try (TaskProcessor p = processor.createTaskProcessor()) {
            int index = 0;
            for (TaskInfo task : tasks) {
                MockTaskProcessorContext context = new MockTaskProcessorContext(String.valueOf(index++), task);
                putResources(context);
                putOutputs(context);
                p.run(context);
            }
        }
    }

    private void doFlatTasks(
            VertexProcessor processor) throws IOException, InterruptedException {
        assert mainInputs.size() == 1;
        String name = mainInputs.keySet().stream().findAny().get();
        List<?> input = mainInputs.get(name);
        List<List<?>> splits = new ArrayList<>();
        if (inputSplit <= 0 || input.isEmpty()) {
            splits.add(input);
        } else {
            int start = 0;
            while (start < input.size()) {
                int end = start + inputSplit;
                splits.add(input.subList(start, end));
                start = end;
            }
        }
        try (TaskProcessor p = processor.createTaskProcessor()) {
            int index = 0;
            for (List<?> split : splits) {
                MockTaskProcessorContext context = new MockTaskProcessorContext(String.valueOf(index++));
                putResources(context);
                context.withInput(name, () -> new CollectionObjectReader(split));
                putOutputs(context);
                p.run(context);
            }
        }
    }

    private void doGroupTasks(VertexProcessor processor) throws IOException, InterruptedException {
        try (TaskProcessor p = processor.createTaskProcessor()) {
            MockTaskProcessorContext context = new MockTaskProcessorContext(String.valueOf(0));
            putResources(context);
            groupInputs.forEach((k, v) -> context.withInput(k, () -> new CollectionGroupReader(v)));
            putOutputs(context);
            p.run(context);
        }
    }

    private void putResources(AbstractProcessorContext<?> context) {
        resources.forEach((c, s) -> context.withResource(c, c.cast(s.get())));
    }

    private void putOutputs(AbstractEdgeIoProcessorContext<?> context) {
        outputs.forEach((n, w) -> context.withOutput(n, () -> w));
    }

    /**
     * Adds a resource to contexts.
     * @param <T> the resource type
     * @param type the resource type
     * @param value the resource
     * @return this
     */
    public <T> VertexProcessorRunner resource(Class<T> type, T value) {
        resources.put(type, () -> value);
        return this;
    }

    /**
     * Adds a main input.
     * @param name the input name
     * @param values the input values
     * @return this
     */
    public VertexProcessorRunner input(String name, Object... values) {
        return input(name, Arrays.asList(values));
    }

    /**
     * Adds a main input.
     * @param name the input name
     * @param values the input values
     * @return this
     */
    public VertexProcessorRunner input(String name, List<?> values) {
        mainInputs
            .computeIfAbsent(name, k -> new ArrayList<>())
            .addAll(values);
        return this;
    }

    /**
     * Adds a main input.
     * @param name the input name
     * @param key the group key
     * @param values the input values
     * @return this
     */
    public VertexProcessorRunner group(String name, Object key, Object... values) {
        return group(name, key, Arrays.asList(values));
    }

    /**
     * Adds a main input.
     * @param name the input name
     * @param key the group key
     * @param values the input values
     * @return this
     */
    public VertexProcessorRunner group(String name, Object key, List<?> values) {
        groupInputs
            .computeIfAbsent(name, k -> new TreeMap<>())
            .computeIfAbsent(key, k -> new ArrayList<>())
            .addAll(values);
        return this;
    }

    /**
     * Configures the input split size.
     * @param size the split size in the number of objects/groups
     * @return this
     */
    public VertexProcessorRunner split(int size) {
        this.inputSplit = size;
        return this;
    }

    /**
     * Adds a side input.
     * @param name the input name
     * @param values the input values
     * @return this
     */
    public VertexProcessorRunner side(String name, Object... values) {
        return side(name, Arrays.asList(values));
    }

    /**
     * Adds a side input.
     * @param name the input name
     * @param values the input values
     * @return this
     */
    public VertexProcessorRunner side(String name, List<?> values) {
        Invariants.require(subInputs.containsKey(name) == false);
        subInputs.put(name, new ArrayList<>(values));
        return this;
    }

    /**
     * Adds an output.
     * @param name the output name
     * @param func the output mapping function
     * @return this
     */
    @SuppressWarnings("unchecked")
    public VertexProcessorRunner output(String name, Function<?, ?> func) {
        Invariants.require(outputs.containsKey(name) == false);
        outputs.put(name, new CollectionObjectWriter((Function<Object, ?>) func));
        return this;
    }

    /**
     * Returns an output.
     * @param name the target output
     * @return objects in the target output
     */
    public List<Object> get(String name) {
        Invariants.require(outputs.containsKey(name));
        return outputs.get(name).getResults();
    }
}
