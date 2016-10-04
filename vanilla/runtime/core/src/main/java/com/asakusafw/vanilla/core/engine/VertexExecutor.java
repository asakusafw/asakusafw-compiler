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
package com.asakusafw.vanilla.core.engine;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.model.PortId;
import com.asakusafw.dag.api.processor.EdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.EdgeReader;
import com.asakusafw.dag.api.processor.EdgeWriter;
import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.ForwardEdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.basic.ForwardProcessorContext;
import com.asakusafw.dag.api.processor.extension.ProcessorContextDecorator;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.InterruptibleIo.Closer;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.vanilla.api.VanillaEdgeDescriptor.Movement;
import com.asakusafw.vanilla.core.mirror.PortMirror;
import com.asakusafw.vanilla.core.mirror.VertexMirror;

/**
 * Executes vertices.
 * @since 0.4.0
 */
public class VertexExecutor implements InterruptibleIo.IoRunnable {

    static final Logger LOG = LoggerFactory.getLogger(VertexExecutor.class);

    private final EdgeIoContext context;

    private final VertexMirror vertex;

    private final ExecutorService executor;

    private final int numberOfThreads;

    private final ProcessorContextDecorator decorator;

    /**
     * Creates a new instance.
     * @param context the root context
     * @param vertex the target vertex
     * @param edges the edge driver
     * @param threads the task executor
     * @param numberOfThreads the number of available {@code threads}
     */
    public VertexExecutor(
            ProcessorContext context,
            VertexMirror vertex,
            EdgeDriver edges,
            ExecutorService threads,
            int numberOfThreads) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(vertex);
        Arguments.requireNonNull(edges);
        Arguments.requireNonNull(threads);
        Arguments.require(numberOfThreads >= 1);
        this.context = new EdgeIoContext(context, vertex, edges);
        this.vertex = vertex;
        this.executor = threads;
        this.numberOfThreads = numberOfThreads;
        this.decorator = context.getResource(ProcessorContextDecorator.class)
                .orElse(ProcessorContextDecorator.NULL);
    }

    @Override
    public void run() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        String label = "N/A"; //$NON-NLS-1$
        try (VertexProcessor processor = vertex.newProcessor(context.getClassLoader())) {
            label = processor.toString();
            List<TaskProcessorContext> tasks = doInitialize(processor);
            doRun(processor, tasks);
            LOG.debug("finalize vertex: {} ({})", label, vertex.getId().getName());
        } catch (Exception e) {
            LOG.error(MessageFormat.format(
                    "vertex execution failed: {1} ({0})",
                    vertex.getId().getName(),
                    label), e);
            throw e;
        }
        context.complete();
        if (LOG.isInfoEnabled()) {
            LOG.info(MessageFormat.format(
                    "finish vertex: {2} ({1}) in {0}ms",
                    System.currentTimeMillis() - start,
                    vertex.getId().getName(),
                    label));
        }
    }

    private List<TaskProcessorContext> doInitialize(
            VertexProcessor processor) throws IOException, InterruptedException {
        VertexProcessorContext vContext = decorator.bless(new VertexContext(context, vertex));

        LOG.debug("initialize vertex: {} ({})", processor, vertex.getId().getName());
        Optional<? extends TaskSchedule> schedule = processor.initialize(vContext);

        // broadcast inputs are only available in VertexProcessor.initialize()
        for (PortMirror port : vertex.getInputs()) {
            if (port.getMovement() == Movement.BROADCAST) {
                context.complete(port.getId());
            }
        }

        LOG.debug("scheduling vertex tasks: {} ({})", processor, vertex.getId().getName());
        List<TaskProcessorContext> results = new ArrayList<>();
        if (schedule.isPresent()) {
            List<? extends TaskInfo> tasks = schedule.get().getTasks();
            int index = 0;
            for (TaskInfo info : tasks) {
                results.add(decorator.bless(new TaskContext(context, vertex, index++, tasks.size(), info)));
            }
        } else {
            int taskCount = computeTaskCount(processor);
            Invariants.require(taskCount >= 1);
            for (int index = 0; index < taskCount; index++) {
                results.add(decorator.bless(new TaskContext(context, vertex, index, taskCount, null)));
            }
        }
        return results;
    }

    private void doRun(
            VertexProcessor processor,
            List<TaskProcessorContext> tasks) throws IOException, InterruptedException {
        int concurrency = computeConcurrency(processor, tasks);
        if (LOG.isDebugEnabled()) {
            LOG.debug("submit tasks: processor={}, vertex={}, tasks={}, threads={}/{}",
                    processor,
                    vertex.getId().getName(),
                    tasks.size(),
                    concurrency,
                    numberOfThreads);
        }
        BlockingQueue<TaskProcessorContext> queue = new LinkedBlockingQueue<>(tasks);
        LinkedList<Future<?>> futures = Lang.let(new LinkedList<>(), it -> Lang.repeat(concurrency, () -> {
            TaskExecutor child = new TaskExecutor(vertex, processor, queue);
            it.add(executor.submit(() -> {
                // this block must be a callable to throw exceptions
                child.run();
                return null;
            }));
        }));
        while (futures.isEmpty() == false) {
            Future<?> first = futures.removeFirst();
            try {
                first.get(100, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("waiting for task completion: {}", first, e); //$NON-NLS-1$
                }
                futures.addLast(first);
            } catch (ExecutionException e) {
                Throwable t = e.getCause();
                Lang.rethrow(t, Error.class);
                Lang.rethrow(t, RuntimeException.class);
                Lang.rethrow(t, IOException.class);
                Lang.rethrow(t, InterruptedException.class);
                throw new IOException(t);
            }
        }
    }

    private int computeTaskCount(VertexProcessor processor) {
        if (vertex.getInputs().stream()
                .map(PortMirror::getMovement)
                .anyMatch(Predicate.isEqual(Movement.SCATTER_GATHER))) {
            return context.getNumberOfPartitions();
        }
        int result = Math.max(numberOfThreads, 1);
        if (processor.getMaxConcurrency() >= 1) {
            result = Math.min(processor.getMaxConcurrency(), result);
        }
        return result;
    }

    private int computeConcurrency(VertexProcessor processor, List<TaskProcessorContext> tasks) {
        int result = Math.min(tasks.size(), numberOfThreads);
        if (processor.getMaxConcurrency() >= 1) {
            result = Math.min(processor.getMaxConcurrency(), result);
        }
        return result;
    }

    private static class EdgeIoContext implements EdgeIoProcessorContext, ForwardProcessorContext {

        private final ProcessorContext forward;

        private final EdgeDriver driver;

        private final Map<String, PortId> inputs;

        private final Map<String, PortId> outputs;

        EdgeIoContext(ProcessorContext forward, VertexMirror vertex, EdgeDriver driver) {
            this.forward = forward;
            this.driver = driver;
            this.inputs = names(vertex.getInputs());
            this.outputs = names(vertex.getOutputs());
        }

        private static Map<String, PortId> names(Collection<? extends PortMirror> ports) {
            return ports.stream()
                    .map(PortMirror::getId)
                    .collect(Collectors.toMap(PortId::getName, Function.identity()));
        }

        @Override
        public ProcessorContext getForward() {
            return forward;
        }

        @Override
        public EdgeReader getInput(String name) throws IOException, InterruptedException {
            return getInput(name, 0, 1);
        }

        EdgeReader getInput(String name, int taskIndex, int taskCount) throws IOException, InterruptedException {
            PortId id = Invariants.requireNonNull(inputs.get(name), name);
            return driver.acquireInput(id, taskIndex, taskCount);
        }

        @Override
        public EdgeWriter getOutput(String name) throws IOException, InterruptedException {
            PortId id = Invariants.requireNonNull(outputs.get(name), name);
            return driver.acquireOutput(id);
        }

        int getNumberOfPartitions() {
            return driver.getNumberOfPartitions();
        }

        void complete(PortId id) throws IOException, InterruptedException {
            driver.complete(id);
        }

        void complete() throws IOException, InterruptedException {
            for (PortId id : inputs.values()) {
                complete(id);
            }
            for (PortId id : outputs.values()) {
                complete(id);
            }
        }
    }

    private static class VertexContext implements VertexProcessorContext, ForwardEdgeIoProcessorContext {

        private final EdgeIoContext forward;

        private final String id;

        VertexContext(EdgeIoContext forward, VertexMirror vertex) {
            this.forward = forward;
            this.id = vertex.getId().getName();
        }

        @Override
        public EdgeIoProcessorContext getForward() {
            return forward;
        }

        @Override
        public String getVertexId() {
            return id;
        }
    }

    private static class TaskContext implements TaskProcessorContext, ForwardEdgeIoProcessorContext {

        private final EdgeIoContext forward;

        private final String vertexId;

        private final String taskId;

        private final int taskIndex;

        private final int taskCount;

        private final TaskInfo info;

        TaskContext(EdgeIoContext forward, VertexMirror vertex, int taskIndex, int taskCount, TaskInfo info) {
            this.forward = forward;
            this.vertexId = vertex.getId().getName();
            this.taskId = String.format("%s-%d", vertexId, taskIndex);
            this.taskIndex = taskIndex;
            this.taskCount = taskCount;
            this.info = info;
        }

        @Override
        public EdgeIoProcessorContext getForward() {
            return forward;
        }

        @Override
        public EdgeReader getInput(String name) throws IOException, InterruptedException {
            return forward.getInput(name, taskIndex, taskCount);
        }

        @Override
        public String getVertexId() {
            return vertexId;
        }

        @Override
        public String getTaskId() {
            return taskId;
        }

        @Override
        public Optional<TaskInfo> getTaskInfo() {
            return Optionals.of(info);
        }
    }

    private static class TaskExecutor implements InterruptibleIo.IoRunnable {

        private final VertexMirror vertex;

        private final VertexProcessor processor;

        private final BlockingQueue<? extends TaskProcessorContext> queue;

        TaskExecutor(
                VertexMirror vertex,
                VertexProcessor processor,
                BlockingQueue<? extends TaskProcessorContext> queue) {
            Arguments.requireNonNull(vertex);
            Arguments.requireNonNull(processor);
            Arguments.requireNonNull(queue);
            this.vertex = vertex;
            this.processor = processor;
            this.queue = queue;
        }

        @Override
        public void run() throws IOException, InterruptedException {
            try (Closer closer = new Closer()) {
                TaskProcessor taskProcessor = null;
                while (true) {
                    TaskProcessorContext next = queue.poll();
                    if (next == null) {
                        break;
                    }
                    if (taskProcessor == null) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("initialize task processor on [{}]: {} ({})",
                                    Thread.currentThread().getName(), processor, vertex.getId().getName());
                        }
                        taskProcessor = closer.add(processor.createTaskProcessor());
                    }
                    LOG.trace("start task: {} ({})", processor, next.getTaskId());
                    taskProcessor.run(next);
                    LOG.trace("finish task: {} ({})", processor, next.getTaskId());
                }
                if (taskProcessor != null) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("finalize task processor on [{}]: {} ({})",
                                Thread.currentThread().getName(), processor, vertex.getId().getName());
                    }
                }
            }
        }
    }
}
