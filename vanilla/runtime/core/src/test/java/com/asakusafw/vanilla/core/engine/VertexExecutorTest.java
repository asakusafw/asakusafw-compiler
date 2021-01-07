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
package com.asakusafw.vanilla.core.engine;

import static com.asakusafw.dag.runtime.testing.MockDataModelUtil.*;
import static com.asakusafw.vanilla.core.testing.ModelMirrors.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.junit.Test;

import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.api.model.PortInfo;
import com.asakusafw.dag.api.model.VertexInfo;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicTaskInfo;
import com.asakusafw.dag.api.processor.basic.BasicTaskSchedule;
import com.asakusafw.dag.runtime.skeleton.VoidVertexProcessor;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockDataModelUtil;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.vanilla.core.mirror.GraphMirror;
import com.asakusafw.vanilla.core.mirror.VertexMirror;
import com.asakusafw.vanilla.core.testing.MockEdgeDriver;

/**
 * Test for {@link VertexExecutor}.
 */
public class VertexExecutorTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        Queue<MockDataModel> results = new ConcurrentLinkedQueue<>();

        GraphInfo graph = new GraphInfo();
        VertexInfo v = graph.addVertex("v", vertex(() -> new SimpleProcessor(results::offer,
                object(0, "0.0", "hello0"))));

        GraphMirror mirror = GraphMirror.of(graph);
        try (MockEdgeDriver edges = new MockEdgeDriver()) {
            run(mirror.getVertex(v.getId()), edges);
            assertThat(sort(results), contains(object(0, "0.0", "hello0")));
        }
    }

    /**
     * w/ multiple tasks.
     * @throws Exception if failed
     */
    @Test
    public void task_multiple() throws Exception {
        List<MockDataModel> inputs = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            inputs.add(object(i, i, "hello" + i));
        }
        Queue<MockDataModel> results = new ConcurrentLinkedQueue<>();

        GraphInfo graph = new GraphInfo();
        VertexInfo v = graph.addVertex("v", vertex(() -> new SimpleProcessor(results::offer, inputs.stream()
                .toArray(MockDataModel[]::new))));

        GraphMirror mirror = GraphMirror.of(graph);
        try (MockEdgeDriver edges = new MockEdgeDriver()) {
            run(mirror.getVertex(v.getId()), edges);
            assertThat(sort(results), equalTo(inputs));
        }
    }

    /**
     * w/ inputs.
     * @throws Exception if failed
     */
    @Test
    public void input() throws Exception {
        Queue<MockDataModel> results = new ConcurrentLinkedQueue<>();

        GraphInfo graph = new GraphInfo();

        VertexInfo dummy = graph.addVertex("dummy", vertex(VoidVertexProcessor.class));
        PortInfo ph = dummy.addOutputPort("ph");

        VertexInfo v = graph.addVertex("v", vertex(() -> new InputProcessor(results::offer)));
        PortInfo input = v.addInputPort(InputProcessor.INPUT_NAME);

        graph.addEdge(ph.getId(), input.getId(), oneToOne(MockDataModelUtil.SerDe.class));

        GraphMirror mirror = GraphMirror.of(graph);
        try (MockEdgeDriver edges = new MockEdgeDriver()) {
            edges.input(input.getId(), Arrays.asList(object(0, "0.0", "hello0")));
            run(mirror.getVertex(v.getId()), edges);
            assertThat(sort(results), contains(
                    object(0, "0.0", "hello0")));
        }
    }

    /**
     * w/ broadcast inputs.
     * @throws Exception if failed
     */
    @Test
    public void input_broadcast() throws Exception {
        Queue<MockDataModel> results = new ConcurrentLinkedQueue<>();

        GraphInfo graph = new GraphInfo();

        VertexInfo dummy = graph.addVertex("dummy", vertex(VoidVertexProcessor.class));
        PortInfo ph = dummy.addOutputPort("ph");

        VertexInfo v = graph.addVertex("v", vertex(() -> new BroadcastProcessor(results::offer)));
        PortInfo input = v.addInputPort(BroadcastProcessor.INPUT_NAME);

        graph.addEdge(ph.getId(), input.getId(), broadcast(MockDataModelUtil.SerDe.class));

        GraphMirror mirror = GraphMirror.of(graph);
        try (MockEdgeDriver edges = new MockEdgeDriver()) {
            edges.broadcast(input.getId(), Arrays.asList(object(0, "0.0", "hello0")));
            run(mirror.getVertex(v.getId()), edges);
            assertThat(sort(results), contains(
                    object(0, "0.0", "hello0")));
        }
    }

    /**
     * w/ outputs.
     * @throws Exception if failed
     */
    @Test
    public void output() throws Exception {
        GraphInfo graph = new GraphInfo();
        VertexInfo v = graph.addVertex("v", vertex(() -> new OutputProcessor(
                object(0, "0.0", "hello0"))));
        PortInfo output = v.addOutputPort(OutputProcessor.OUTPUT_NAME);

        VertexInfo dummy = graph.addVertex("dummy", vertex(VoidVertexProcessor.class));
        PortInfo drop = dummy.addInputPort("drop");
        graph.addEdge(output.getId(), drop.getId(), oneToOne(MockDataModelUtil.SerDe.class));

        GraphMirror mirror = GraphMirror.of(graph);
        try (MockEdgeDriver edges = new MockEdgeDriver()) {
            edges.output(output.getId(), (UnaryOperator<MockDataModel>) MockDataModel::new);
            run(mirror.getVertex(v.getId()), edges);
            assertThat(sort(edges.get(MockDataModel.class, output.getId())), contains(
                    object(0, "0.0", "hello0")));
        }
    }

    private void run(VertexMirror vertex, MockEdgeDriver edges) throws IOException, InterruptedException {
        ProcessorContext context = new BasicProcessorContext(getClass().getClassLoader());
        int concurrency = Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
        ExecutorService threads = Executors.newFixedThreadPool(concurrency, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        try {
            VertexExecutor executor = new VertexExecutor(context, vertex, edges, threads, concurrency);
            executor.run();
            assertThat(edges.isCompleted(), is(true));
        } finally {
            threads.shutdownNow();
        }
    }

    private static class SimpleProcessor implements VertexProcessor {

        private final List<MockDataModel> inputs;

        private final Consumer<MockDataModel> sink;

        SimpleProcessor(Consumer<MockDataModel> sink, MockDataModel... values) {
            this.inputs = Arrays.asList(values);
            this.sink = sink;
        }

        @Override
        public Optional<? extends TaskSchedule> initialize(VertexProcessorContext context) {
            return Optionals.of(new BasicTaskSchedule(inputs.stream()
                    .map(BasicTaskInfo::new)
                    .collect(Collectors.toList())));
        }

        @Override
        public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
            return c -> c.getTaskInfo()
                    .map(BasicTaskInfo.class::cast)
                    .flatMap(i -> Optionals.of(i.getValue()))
                    .map(MockDataModel.class::cast)
                    .ifPresent(sink);
        }

        @Override
        public String toString() {
            return String.format("Simple(%,d)", inputs.size());
        }
    }

    private static class InputProcessor implements VertexProcessor {

        static final String INPUT_NAME = "input";

        private final Consumer<MockDataModel> sink;

        InputProcessor(Consumer<MockDataModel> sink) {
            this.sink = sink;
        }

        @Override
        public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
            return c -> {
                try (ObjectReader reader = (ObjectReader) c.getInput(INPUT_NAME)) {
                    reader.forEach(MockDataModel.class, sink);
                }
            };
        }

        @Override
        public String toString() {
            return String.format("Input()");
        }
    }

    private static class OutputProcessor implements VertexProcessor {

        static final String OUTPUT_NAME = "output";

        private final List<MockDataModel> inputs;

        OutputProcessor(MockDataModel... values) {
            this.inputs = Arrays.asList(values);
        }

        @Override
        public Optional<? extends TaskSchedule> initialize(VertexProcessorContext context) {
            return Optionals.of(new BasicTaskSchedule(inputs.stream()
                    .map(BasicTaskInfo::new)
                    .collect(Collectors.toList())));
        }

        @Override
        public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
            return c -> {
                try (ObjectWriter writer = (ObjectWriter) c.getOutput(OUTPUT_NAME)) {
                    Object o = c.getTaskInfo()
                        .map(BasicTaskInfo.class::cast)
                        .flatMap(i -> Optionals.of(i.getValue()))
                        .get();
                    writer.putObject(o);
                }
            };
        }

        @Override
        public String toString() {
            return String.format("Output(%,d)", inputs.size());
        }
    }

    private static class BroadcastProcessor implements VertexProcessor {

        static final String INPUT_NAME = "broadcast";

        private final Consumer<MockDataModel> sink;

        BroadcastProcessor(Consumer<MockDataModel> sink) {
            this.sink = sink;
        }

        @Override
        public Optional<? extends TaskSchedule> initialize(
                VertexProcessorContext context) throws IOException, InterruptedException {
            try (ObjectReader reader = (ObjectReader) context.getInput(INPUT_NAME)) {
                reader.forEach(MockDataModel.class, sink);
            }
            return Optionals.of(new BasicTaskSchedule());
        }

        @Override
        public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return String.format("Broadcast()");
        }
    }
}
