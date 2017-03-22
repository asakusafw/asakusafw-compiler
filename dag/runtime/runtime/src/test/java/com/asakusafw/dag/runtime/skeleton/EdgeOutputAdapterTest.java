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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockTaskProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.ContextHandler.Session;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.adapter.ObjectCombiner;
import com.asakusafw.dag.runtime.adapter.ObjectCopier;
import com.asakusafw.dag.runtime.adapter.OutputHandler;
import com.asakusafw.dag.runtime.skeleton.EdgeOutputAdapterTest.Pair.PairCombiner;
import com.asakusafw.dag.runtime.skeleton.EdgeOutputAdapterTest.Pair.PairCopier;
import com.asakusafw.dag.runtime.skeleton.EdgeOutputAdapterTest.Pair.ToPairMapper;
import com.asakusafw.dag.runtime.skeleton.EdgeOutputHandler.AggregationStrategy;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.lang.utils.common.Tuple;
import com.asakusafw.runtime.core.Result;

/**
 * Test for {@link EdgeOutputAdapter}.
 */
public class EdgeOutputAdapterTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Map<String, List<Object>> in = new LinkedHashMap<>();
        in.put("o0", Arrays.asList("Hello, world!"));

        check(in);
    }

    /**
     * multiple outputs.
     */
    @Test
    public void values() {
        Map<String, List<Object>> in = new LinkedHashMap<>();
        in.put("o0", Arrays.asList("A", "B", "C"));

        check(in);
    }

    /**
     * multiple outputs.
     */
    @Test
    public void entries() {
        Map<String, List<Object>> in = new LinkedHashMap<>();
        in.put("o0", Arrays.asList("A0"));
        in.put("o1", Arrays.asList("A1", "B1"));
        in.put("o2", Arrays.asList("A2", "B2", "C2"));

        check(in);
    }

    private void check(Map<String, List<Object>> map) {
        MockTaskProcessorContext tc = new MockTaskProcessorContext("t");
        Map<String, List<Object>> results = map.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), s -> new ArrayList<>()));
        results.forEach((o, v) -> tc.withOutput(o, v::add));
        try (EdgeOutputAdapter adapter = new EdgeOutputAdapter(new MockVertexProcessorContext())) {
            map.keySet().forEach(s -> adapter.bind(s));
            adapter.initialize();

            OutputHandler<? super TaskProcessorContext> handler = adapter.newHandler();
            List<Runnable> actions = Lang.project(map.entrySet(),
                    e -> {
                        Result<Object> r = handler.getSink(Object.class, e.getKey());
                        return (Runnable) () -> e.getValue().forEach(r::add);
                    });
            try (Session s = handler.start(tc)) {
                actions.forEach(Runnable::run);
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertThat(results, is(map));
    }

    /**
     * w/ mapping.
     */
    @Test
    public void mapping() {
        List<Object> results = new ArrayList<>();
        MockTaskProcessorContext tc = new MockTaskProcessorContext("t")
                .withOutput("o", results::add);
        try (EdgeOutputAdapter adapter = new EdgeOutputAdapter(new MockVertexProcessorContext())) {
            adapter.bind("o", SimpleMapper.class, null, null);
            adapter.initialize();
            OutputHandler<? super TaskProcessorContext> handler = adapter.newHandler();
            Result<String> r = handler.getSink(String.class, "o");
            try (Session s = handler.start(tc)) {
                r.add("testing");
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertThat(results, contains("testing!"));
    }

    @SuppressWarnings("javadoc")
    public static class SimpleMapper implements Function<String, String> {
        @Override
        public String apply(String t) {
            return String.valueOf(t) + "!";
        }
    }

    /**
     * w/ custom window size.
     */
    @Test
    public void window_size() {
        try (EdgeOutputAdapter adapter = new EdgeOutputAdapter(new MockVertexProcessorContext()
                .withProperty(EdgeOutputAdapter.KEY_AGGREGATION_WINDOW_SIZE, "123"))) {
            assertThat(adapter.aggregationWindowSize, is(123));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * w/ custom window size.
     */
    @Test
    public void aggregate_strategy() {
        try (EdgeOutputAdapter adapter = new EdgeOutputAdapter(new MockVertexProcessorContext()
                .withProperty(EdgeOutputAdapter.KEY_AGGREGATION_STRATEGY, AggregationStrategy.HASH.name()))) {
            assertThat(adapter.aggregationStrategy, is(AggregationStrategy.HASH));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * w/ combining.
     */
    @Test
    public void combining() {
        combining0(AggregationStrategy.MAP);
    }

    /**
     * w/ combining.
     */
    @Test
    public void combining_hash() {
        combining0(AggregationStrategy.HASH);
    }

    @SuppressWarnings("unchecked")
    private static void combining0(AggregationStrategy strategy) {
        List<Object> results = new ArrayList<>();
        MockTaskProcessorContext tc = new MockTaskProcessorContext("t")
                .withOutput("o", results::add);
        try (EdgeOutputAdapter adapter = new EdgeOutputAdapter(new MockVertexProcessorContext()
                .withProperty(EdgeOutputAdapter.KEY_AGGREGATION_STRATEGY, strategy.name()))) {
            adapter.bind("o", null, PairCopier.class, PairCombiner.class);
            adapter.initialize();
            OutputHandler<? super TaskProcessorContext> handler = adapter.newHandler();
            Result<Pair> r = handler.getSink(Pair.class, "o");
            try (Session s = handler.start(tc)) {
                r.add(new Pair(0, 1));
                r.add(new Pair(1, 1));
                r.add(new Pair(2, 1));
                r.add(new Pair(1, 2));
                r.add(new Pair(2, 2));
                r.add(new Pair(2, 4));
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertThat(Lang.project(results, o -> ((Pair) o).toTuple()),
                containsInAnyOrder(new Tuple<>(0, 1), new Tuple<>(1, 3), new Tuple<>(2, 7)));
    }

    /**
     * w/ mapping + combining.
     */
    @Test
    public void mapping_combining() {
        mapping_combining0(AggregationStrategy.MAP);
    }

    /**
     * w/ mapping + combining.
     */
    @Test
    public void mapping_combining_hash() {
        mapping_combining0(AggregationStrategy.HASH);
    }

    @SuppressWarnings("unchecked")
    private static void mapping_combining0(AggregationStrategy strategy) {
        List<Object> results = new ArrayList<>();
        MockTaskProcessorContext tc = new MockTaskProcessorContext("t")
                .withOutput("o", results::add);
        try (EdgeOutputAdapter adapter = new EdgeOutputAdapter(new MockVertexProcessorContext()
                .withProperty(EdgeOutputAdapter.KEY_AGGREGATION_STRATEGY, strategy.name()))) {
            adapter.bind("o", ToPairMapper.class, PairCopier.class, PairCombiner.class);
            adapter.initialize();
            OutputHandler<? super TaskProcessorContext> handler = adapter.newHandler();
            Result<Object> r = handler.getSink(Object.class, "o");
            try (Session s = handler.start(tc)) {
                r.add(new Tuple<>(0, 1));
                r.add(new Tuple<>(1, 1));
                r.add(new Tuple<>(2, 1));
                r.add(new Tuple<>(1, 2));
                r.add(new Tuple<>(2, 2));
                r.add(new Tuple<>(2, 4));
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertThat(Lang.project(results, o -> ((Pair) o).toTuple()),
                containsInAnyOrder(new Tuple<>(0, 1), new Tuple<>(1, 3), new Tuple<>(2, 7)));
    }

    /**
     * w/ combining.
     */
    @Test
    public void combining_flush() {
        combining_flush0(100, AggregationStrategy.MAP);
    }

    /**
     * w/ combining.
     */
    @Test
    public void combining_flush_hash() {
        combining_flush0(100, AggregationStrategy.HASH);
    }

    private static void combining_flush0(int windowSize, AggregationStrategy strategy) {
        List<Object> results = new ArrayList<>();
        MockTaskProcessorContext tc = new MockTaskProcessorContext("t")
                .withOutput("o", results::add);
        try (EdgeOutputAdapter adapter = new EdgeOutputAdapter(new MockVertexProcessorContext()
                .withProperty(EdgeOutputAdapter.KEY_AGGREGATION_WINDOW_SIZE, String.valueOf(windowSize))
                .withProperty(EdgeOutputAdapter.KEY_AGGREGATION_STRATEGY, strategy.name()))) {
            adapter.bind("o", null, PairCopier.class, PairCombiner.class);
            adapter.initialize();
            OutputHandler<? super TaskProcessorContext> handler = adapter.newHandler();
            Result<Pair> r = handler.getSink(Pair.class, "o");
            try (Session s = handler.start(tc)) {
                for (int i = 0; i < windowSize * 10; i++) {
                    r.add(new Pair(i % (windowSize + 1), i));
                }
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertThat(results, hasSize(greaterThan(windowSize)));
    }

    @SuppressWarnings("javadoc")
    public static class Pair {

        final int left;

        int right;

        public Pair(int a, int b) {
            this.left = a;
            this.right = b;
        }

        public Tuple<Integer, Integer> toTuple() {
            return new Tuple<>(left, right);
        }

        public static class ToPairMapper implements Function<Tuple<Integer, Integer>, Pair> {
            @Override
            public Pair apply(Tuple<Integer, Integer> t) {
                return new Pair(t.left(), t.right());
            }
        }

        public static class PairCopier implements ObjectCopier<Pair> {
            @Override
            public Pair newCopy(Pair source) {
                return new Pair(source.left, source.right);
            }
        }

        public static class PairCombiner implements ObjectCombiner<Pair> {
            @Override
            public void buildKey(KeyBuffer key, Pair object) {
                key.append(new IntWritable(object.left));
            }
            @Override
            public void combine(Pair a, Pair b) {
                a.right += b.right;
            }
        }
    }
}
