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
package com.asakusafw.vanilla.core.engine;

import static com.asakusafw.dag.runtime.testing.MockDataModelUtil.*;
import static com.asakusafw.vanilla.core.testing.ModelMirrors.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.UnaryOperator;

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
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockDataModelUtil;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.vanilla.core.mirror.GraphMirror;
import com.asakusafw.vanilla.core.testing.MockEdgeDriver;

/**
 * Test for {@link GraphExecutor}.
 */
public class GraphExecutorTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        List<MockDataModel> inputs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            inputs.add(object(i, i, "hello" + i));
        }
        Queue<MockDataModel> outputs = new ConcurrentLinkedQueue<>();

        GraphInfo graph = new GraphInfo();
        VertexInfo v0 = graph.addVertex("v0", vertex(() -> new VertexProcessor() {
            @Override
            public Optional<? extends TaskSchedule> initialize(VertexProcessorContext context) {
                return Optionals.of(new BasicTaskSchedule(new BasicTaskInfo()));
            }
            @Override
            public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
                return c -> {
                    try (ObjectWriter writer = (ObjectWriter) c.getOutput("port")) {
                        for (MockDataModel o : inputs) {
                            writer.putObject(o);
                        }
                    }
                };
            }
        }));
        VertexInfo v1 = graph.addVertex("v1", vertex(() -> new VertexProcessor() {
            @Override
            public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
                return c -> {
                    try (ObjectReader reader = (ObjectReader) c.getInput("port")) {
                        reader.forEach(MockDataModel.class, outputs::offer);
                    }
                };
            }
        }));

        PortInfo v0out = v0.addOutputPort("port");
        PortInfo v1in = v1.addInputPort("port");
        graph.addEdge(v0out.getId(), v1in.getId(), oneToOne(MockDataModelUtil.SerDe.class));

        GraphMirror mirror = GraphMirror.of(graph);
        try (MockEdgeDriver edges = new MockEdgeDriver()) {
            edges.output(v0out.getId(), (UnaryOperator<MockDataModel>) MockDataModel::new);
            edges.input(v1in.getId(), inputs);
            run(mirror, edges);
            assertThat(sort(edges.get(MockDataModel.class, v0out.getId())), is(inputs));
            assertThat(sort(outputs), is(inputs));
        }
    }

    private void run(GraphMirror mirror, MockEdgeDriver edges) throws IOException, InterruptedException {
        ProcessorContext context = new BasicProcessorContext(getClass().getClassLoader());
        VertexScheduler sched = new BasicVertexScheduler();
        int concurrency = Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
        GraphExecutor executor = new GraphExecutor(context, mirror, sched, edges, concurrency);
        executor.run();
        assertThat(edges.isCompleted(), is(true));
    }
}
