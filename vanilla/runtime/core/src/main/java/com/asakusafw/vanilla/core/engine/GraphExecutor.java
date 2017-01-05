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
package com.asakusafw.vanilla.core.engine;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.vanilla.core.mirror.GraphMirror;
import com.asakusafw.vanilla.core.mirror.PortMirror;
import com.asakusafw.vanilla.core.mirror.VertexMirror;

/**
 * Executes graphs.
 * @since 0.4.0
 */
public class GraphExecutor implements InterruptibleIo.IoRunnable {

    static final Logger LOG = LoggerFactory.getLogger(GraphExecutor.class);

    private final ProcessorContext context;

    private final GraphMirror graph;

    private final VertexScheduler scheduler;

    private final EdgeDriver edges;

    private final int numberOfThreads;

    /**
     * Creates a new instance.
     * @param context the root context
     * @param graph the target graph
     * @param scheduler the scheduler
     * @param edges the edge driver
     * @param numberOfThreads the number of available {@code threads}
     */
    public GraphExecutor(
            ProcessorContext context, GraphMirror graph,
            VertexScheduler scheduler, EdgeDriver edges, int numberOfThreads) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(graph);
        Arguments.requireNonNull(scheduler);
        Arguments.requireNonNull(edges);
        Arguments.require(numberOfThreads >= 1);
        this.context = context;
        this.graph = graph;
        this.scheduler = scheduler;
        this.edges = edges;
        this.numberOfThreads = numberOfThreads;
    }

    @Override
    public void run() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        int numberOfVertices = graph.getVertices().size();
        LOG.info(MessageFormat.format(
                "start graph: vertices={0}",
                numberOfVertices));
        Set<VertexMirror> finished = new HashSet<>();
        try (VertexScheduler.Stream schedule = scheduler.schedule(graph);
                ThreadPool threads = new ThreadPool(numberOfThreads)) {
            while (true) {
                VertexMirror vertex = schedule.poll();
                if (vertex == null) {
                    break;
                }
                Invariants.require(finished.contains(vertex) == false);
                Invariants.require(vertex.getInputs().stream()
                        .flatMap(p -> p.getOpposites().stream())
                        .map(PortMirror::getOwner)
                        .allMatch(finished::contains));
                VertexExecutor child = new VertexExecutor(context, vertex, edges, threads.executor, numberOfThreads);
                child.run();
                finished.add(vertex);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("completed: vertices={}/{} ({})", finished.size(), numberOfVertices, edges);
                }
            }
        }
        Invariants.require(finished.size() == numberOfVertices);
        LOG.info(MessageFormat.format(
                "finish graph: vertices={0}, elapsed={1}ms",
                numberOfVertices,
                System.currentTimeMillis() - start));
    }

    private static final class ThreadPool implements AutoCloseable {

        final ExecutorService executor;

        ThreadPool(int numberOfThreads) {
            AtomicInteger counter = new AtomicInteger();
            this.executor = Executors.newFixedThreadPool(
                    numberOfThreads,
                    r -> Lang.let(new Thread(r), t -> {
                        t.setName(String.format("vanilla-%d", counter.incrementAndGet())); //$NON-NLS-1$
                        t.setDaemon(true);
                    }));
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
