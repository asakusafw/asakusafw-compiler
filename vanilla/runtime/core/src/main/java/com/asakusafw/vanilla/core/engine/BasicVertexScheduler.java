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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;
import com.asakusafw.vanilla.api.VanillaEdgeDescriptor.Movement;
import com.asakusafw.vanilla.core.mirror.GraphMirror;
import com.asakusafw.vanilla.core.mirror.InputPortMirror;
import com.asakusafw.vanilla.core.mirror.OutputPortMirror;
import com.asakusafw.vanilla.core.mirror.PortMirror;
import com.asakusafw.vanilla.core.mirror.VertexMirror;
import com.asakusafw.vanilla.core.util.SystemProperty;

/**
 * A basic implementation of {@link VertexScheduler}.
 * @since 0.4.0
 */
public class BasicVertexScheduler implements VertexScheduler {

    static final Logger LOG = LoggerFactory.getLogger(BasicVertexScheduler.class);

    static final String KEY_PREFIX = "com.asakusafw.vanilla.scheduler."; //$NON-NLS-1$

    /**
     * The system property key of each input port score ({@value}: {@value #DEFAULT_INPUT_SCORE}).
     * @see #KEY_SHARED_INPUT_SCALE
     * @see #KEY_IMMEDIATE_INPUT_SCALE
     */
    public static final String KEY_INPUT_SCORE = KEY_PREFIX + "score.input"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_INPUT_SCORE} ({@value}).
     */
    public static final double DEFAULT_INPUT_SCORE = 1.0;

    /**
     * The system property key of each output port score ({@value}: {@value #DEFAULT_OUTPUT_SCORE}).
     */
    public static final String KEY_OUTPUT_SCORE = KEY_PREFIX + "score.output"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_OUTPUT_SCORE} ({@value}).
     */
    public static final double DEFAULT_OUTPUT_SCORE = -1.0;

    /**
     * The system property key of score factor of the rest critical path length
     * ({@value}: {@value #DEFAULT_LENGTH_SCORE}).
     */
    public static final String KEY_LENGTH_SCORE = KEY_PREFIX + "score.length"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_LENGTH_SCORE} ({@value}).
     */
    public static final double DEFAULT_LENGTH_SCORE = -0.25;

    /**
     * The system property key of score factor of the succeeding vertices' score
     * ({@value}: {@value #DEFAULT_SUCCESSOR_SCALE}).
     */
    public static final String KEY_SUCCESSOR_SCALE = KEY_PREFIX + "score.successor"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_SUCCESSOR_SCALE} ({@value}).
     */
    public static final double DEFAULT_SUCCESSOR_SCALE = 0.25;

    /**
     * The system property key of input port score scale factor which input shares the source buffer with other inputs
     * ({@value}: {@value #DEFAULT_SHARED_INPUT_SCALE}).
     */
    public static final String KEY_SHARED_INPUT_SCALE = KEY_PREFIX + "score.input.shared"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_SHARED_INPUT_SCALE} ({@value}).
     */
    public static final double DEFAULT_SHARED_INPUT_SCALE = 0.5;

    /**
     * The system property key of input port score scale factor which input was generated by the last vertex
     * ({@value}: {@value #DEFAULT_IMMEDIATE_INPUT_SCALE}).
     */
    public static final String KEY_IMMEDIATE_INPUT_SCALE = KEY_PREFIX + "score.input.immediate"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_IMMEDIATE_INPUT_SCALE} ({@value}).
     */
    public static final double DEFAULT_IMMEDIATE_INPUT_SCALE = 1.1;

    static final double INPUT_SCORE = SystemProperty.get(KEY_INPUT_SCORE, DEFAULT_INPUT_SCORE);

    static final double OUTPUT_SCORE = SystemProperty.get(KEY_OUTPUT_SCORE, DEFAULT_OUTPUT_SCORE);

    static final double LENGTH_SCORE = SystemProperty.get(KEY_LENGTH_SCORE, DEFAULT_LENGTH_SCORE);

    static final double SUCCESSOR_SCALE = SystemProperty.get(KEY_SUCCESSOR_SCALE, DEFAULT_SUCCESSOR_SCALE);

    static final double SHARED_INPUT_SCALE = SystemProperty.get(KEY_SHARED_INPUT_SCALE, DEFAULT_SHARED_INPUT_SCALE);

    static final double IMMEDIATE_INPUT_SCALE =
            SystemProperty.get(KEY_IMMEDIATE_INPUT_SCALE, DEFAULT_IMMEDIATE_INPUT_SCALE);

    static {
        if (LOG.isDebugEnabled()) {
            LOG.debug("scheduler:");
            LOG.debug("  {}: {}", KEY_INPUT_SCORE, INPUT_SCORE);
            LOG.debug("  {}: {}", KEY_OUTPUT_SCORE, OUTPUT_SCORE);
            LOG.debug("  {}: {}", KEY_LENGTH_SCORE, LENGTH_SCORE);
            LOG.debug("  {}: {}", KEY_SUCCESSOR_SCALE, SUCCESSOR_SCALE);
            LOG.debug("  {}: {}", KEY_SHARED_INPUT_SCALE, SHARED_INPUT_SCALE);
            LOG.debug("  {}: {}", KEY_IMMEDIATE_INPUT_SCALE, IMMEDIATE_INPUT_SCALE);
        }
    }

    @Override
    public VertexScheduler.Stream schedule(GraphMirror graph) {
        return new Stream(graph.getVertices());
    }

    private static final class Stream implements VertexScheduler.Stream {

        private final Map<VertexMirror, Schedule> waiting;

        private final LinkedList<Schedule> staged = new LinkedList<>();

        private final Set<VertexMirror> scheduled = new HashSet<>();

        private VertexMirror lastScheduled;

        Stream(Collection<? extends VertexMirror> vertices) {
            this.waiting = build(vertices);
            for (Iterator<Schedule> iter = waiting.values().iterator(); iter.hasNext();) {
                Schedule next = iter.next();
                if (next.vertex.getInputs().isEmpty()) {
                    iter.remove();
                    staged.add(next);
                }
            }
        }

        private static Map<VertexMirror, Schedule> build(Collection<? extends VertexMirror> vertices) {
            Graph<VertexMirror> dag = Graphs.newInstance();
            for (VertexMirror vertex : vertices) {
                dag.addNode(vertex);
                vertex.getOutputs().stream()
                    .flatMap(p -> p.getOpposites().stream())
                    .map(PortMirror::getOwner)
                    .forEach(successor -> dag.addEdge(vertex, successor));
            }
            assert Graphs.findCircuit(dag).isEmpty();

            Map<VertexMirror, Schedule> results = new HashMap<>();
            for (VertexMirror vertex : Graphs.sortPostOrder(dag)) {
                assert results.containsKey(vertex) == false;
                List<Schedule> successors = new ArrayList<>();
                for (VertexMirror successor : dag.getConnected(vertex)) {
                    assert results.containsKey(successor);
                    successors.add(results.get(successor));
                }
                results.put(vertex, new Schedule(vertex, successors));
            }

            assert results.size() == vertices.size();
            return results;
        }

        @Override
        public VertexMirror poll() throws IOException, InterruptedException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("start scheduling: waiting={}, staged={}, completed={}",
                        waiting.size(), staged.size(), scheduled.size());
            }
            int index = select();
            if (index < 0) {
                assert waiting.isEmpty();
                assert staged.isEmpty();
                lastScheduled = null;
                return null;
            }
            Schedule next = staged.remove(index);
            if (LOG.isDebugEnabled()) {
                LOG.debug("next: {}", next);
            }
            return complete(next.vertex);
        }

        private int select() {
            Schedule candidate = null;
            int candidateIndex = -1;
            int index = 0;
            for (Schedule schedule : staged) {
                schedule.update(lastScheduled, scheduled);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("scheduling: {}", schedule);
                }
                if (candidate == null || schedule.isBetterThan(candidate)) {
                    candidate = schedule;
                    candidateIndex = index;
                }
                index++;
            }
            return candidateIndex;
        }

        private VertexMirror complete(VertexMirror vertex) {
            scheduled.add(vertex);
            lastScheduled = vertex;
            List<VertexMirror> ready = vertex.getOutputs().stream()
                .map(PortMirror::getOpposites)
                .flatMap(Collection::stream)
                .map(PortMirror::getOwner)
                .filter(waiting::containsKey)
                .distinct()
                .filter(v -> v.getInputs().stream()
                        .map(PortMirror::getOpposites)
                        .flatMap(Collection::stream)
                        .map(PortMirror::getOwner)
                        .allMatch(scheduled::contains))
                .collect(Collectors.toList());
            ready.forEach(v -> staged.add(Invariants.requireNonNull(waiting.remove(v))));
            return vertex;
        }

        @Override
        public void close() {
            return;
        }
    }

    private static final class Schedule {

        final VertexMirror vertex;

        final List<OutputPortMirror> upstreams;

        final int length;

        final double baseScore;

        double score = 0;

        Schedule(VertexMirror vertex, List<Schedule> successors) {
            this.vertex = vertex;
            this.upstreams = vertex.getInputs().stream()
                    .filter(p -> p.getMovement() != Movement.NOTHING)
                    .map(InputPortMirror::getOpposites)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            this.length = successors.stream()
                    .mapToInt(s -> s.length)
                    .max()
                    .orElse(-1) + 1;

            double base = 0.0;
            base += upstreams.size() * INPUT_SCORE;
            base += vertex.getOutputs().stream()
                    .filter(p -> p.getMovement() != Movement.NOTHING)
                    .count() * OUTPUT_SCORE;
            base += length * LENGTH_SCORE;
            base += successors.stream()
                    .mapToDouble(s -> s.baseScore)
                    .sum() * SUCCESSOR_SCALE;
            this.baseScore = base;
        }

        void update(VertexMirror last, Set<VertexMirror> completed) {
            double delta = upstreams.stream()
                    .mapToDouble(p -> {
                        double d = INPUT_SCORE;
                        if (isShared(p, completed)) {
                            d *= SHARED_INPUT_SCALE;
                        }
                        if (p.getOwner() == last) {
                            d *= IMMEDIATE_INPUT_SCALE;
                        }
                        return d - INPUT_SCORE;
                    })
                    .sum();
            this.score = baseScore + delta;
        }

        private boolean isShared(OutputPortMirror upstream, Set<VertexMirror> completed) {
            // whether or not the given upstream port has any incomplete successors other than this vertex
            return upstream.getOpposites().stream()
                    .map(PortMirror::getOwner)
                    .filter(v -> v != vertex)
                    .anyMatch(v -> completed.contains(v) == false);
        }

        boolean isBetterThan(Schedule o) {
            if (this == o) {
                return false;
            }
            int scoreDiff = Double.compare(score, o.score);
            if (scoreDiff != 0) {
                return scoreDiff > 0;
            }
            return vertex.getId().getName().compareTo(o.vertex.getId().getName()) < 0;
        }

        @Override
        public String toString() {
            return String.format("Schedule(%s=%,f)", vertex.getId().getName(), score);
        }
    }
}
