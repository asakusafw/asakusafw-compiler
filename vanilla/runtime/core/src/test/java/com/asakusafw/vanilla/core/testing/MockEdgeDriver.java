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
package com.asakusafw.vanilla.core.testing;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.asakusafw.dag.api.common.ObjectCursor;
import com.asakusafw.dag.api.model.PortId;
import com.asakusafw.dag.api.model.PortInfo.Direction;
import com.asakusafw.dag.api.processor.EdgeReader;
import com.asakusafw.dag.api.processor.EdgeWriter;
import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Suppliers;
import com.asakusafw.lang.utils.common.Tuple;
import com.asakusafw.vanilla.core.engine.EdgeDriver;
import com.asakusafw.vanilla.core.engine.VertexExecutor;

/**
 * Mock implementation of {@link EdgeDriver}.
 * This is only for testing individual vertices, because {@link #complete(PortId)} will never exchange datasets
 * over edges. Clients should put each input, and finally can obtain output data by using {@link #get(Class, PortId)}.
 * @since 0.4.0
 * @see VertexExecutor
 */
public class MockEdgeDriver implements EdgeDriver {

    private final Map<PortId, Supplier<? extends EdgeReader>> inputs = new LinkedHashMap<>();

    private final Map<PortId, Supplier<? extends EdgeWriter>> outputs = new LinkedHashMap<>();

    private final Map<PortId, Queue<?>> sinks = new HashMap<>();

    /**
     * Adds a non-broadcast input.
     * @param id the port ID
     * @param values the values
     * @return this
     */
    public MockEdgeDriver input(PortId id, List<?> values) {
        Invariants.require(inputs.containsKey(id) == false);
        Queue<Object> q = new ConcurrentLinkedQueue<>(values);
        inputs.put(id, () -> new SupplierObjectReader(q::poll));
        return this;
    }

    /**
     * Adds a group input.
     * @param <K> the key type
     * @param id the port ID
     * @param values the values
     * @return this
     */
    public <K> MockEdgeDriver input(PortId id, SortedMap<K, ? extends List<?>> values) {
        Invariants.require(inputs.containsKey(id) == false);
        Queue<Tuple<K, List<?>>> q = values.entrySet().stream()
                .sequential()
                .map(e -> Tuple.<K, List<?>>of(e))
                .collect(Collectors.toCollection(LinkedBlockingQueue::new));
        inputs.put(id, () -> new SupplierGroupReader<>(q::poll, values.comparator()));
        return this;
    }

    /**
     * Adds a broadcast input.
     * @param id the port ID
     * @param values the values
     * @return this
     */
    public MockEdgeDriver broadcast(PortId id, List<?> values) {
        Invariants.require(inputs.containsKey(id) == false);
        inputs.put(id, () -> new SupplierObjectReader(Suppliers.fromIterable(values)));
        return this;
    }

    /**
     * Adds an output.
     * @param <T> the value type
     * @param id the port ID
     * @param copier the object copier
     * @return this
     */
    @SuppressWarnings("unchecked")
    public <T> MockEdgeDriver output(PortId id, UnaryOperator<T> copier) {
        Invariants.require(outputs.containsKey(id) == false);
        Queue<T> sink = new LinkedBlockingQueue<>();
        outputs.put(id, () -> new ObjectWriterConsumer(v -> sink.offer(copier.apply((T) v))));
        sinks.put(id, sink);
        return this;
    }

    /**
     * Returns the output objects.
     * @param <T> the value type
     * @param type the value type
     * @param id the port ID
     * @return the output objects
     */
    public <T> List<T> get(Class<T> type, PortId id) {
        Invariants.require(sinks.containsKey(id));
        return sinks.get(id).stream()
                .map(type::cast)
                .collect(Collectors.toList());
    }

    /**
     * Returns whether or not this driver has been already completed.
     * @return {@code true} if this has been completed, otherwise {@code false}
     */
    public boolean isCompleted() {
        return inputs.isEmpty() && outputs.isEmpty();
    }

    @Override
    public int getNumberOfPartitions() {
        return 1;
    }

    @Override
    public EdgeReader acquireInput(PortId id, int taskIndex, int taskCount) throws IOException, InterruptedException {
        Invariants.require(inputs.containsKey(id));
        return inputs.get(id).get();
    }

    @Override
    public EdgeWriter acquireOutput(PortId id) throws IOException, InterruptedException {
        Invariants.require(outputs.containsKey(id));
        return outputs.get(id).get();
    }

    @Override
    public void complete(PortId id) throws IOException, InterruptedException {
        if (id.getDirection() == Direction.INPUT) {
            inputs.remove(id);
        } else if (id.getDirection() == Direction.OUTPUT) {
            outputs.remove(id);
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        return;
    }

    static class SupplierObjectReader implements ObjectReader {

        private final Supplier<?> supplier;

        private Object next;

        SupplierObjectReader(Supplier<?> supplier) {
            this.supplier = supplier;
        }

        @Override
        public boolean nextObject() throws IOException, InterruptedException {
            Object n = supplier.get();
            if (n == null) {
                next = null;
                return false;
            } else {
                next = n;
                return true;
            }
        }

        @Override
        public Object getObject() throws IOException, InterruptedException {
            return Invariants.requireNonNull(next);
        }
    }

    static class SupplierGroupReader<K> implements GroupReader {

        private final Supplier<? extends Tuple<K, ? extends Iterable<?>>> supplier;

        private final Group<K> current;

        SupplierGroupReader(
                Supplier<? extends Tuple<K, ? extends Iterable<?>>> supplier,
                Comparator<? super K> comparator) {
            this.supplier = supplier;
            this.current = new Group<>(comparator);
        }

        @Override
        public boolean nextGroup() throws IOException, InterruptedException {
            Tuple<K, ? extends Iterable<?>> next = supplier.get();
            if (next == null) {
                current.key = null;
                current.values = null;
                return false;
            } else {
                current.key = next.left();
                current.values = Suppliers.fromIterable(next.right());
                return true;
            }
        }

        @Override
        public GroupInfo getGroup() throws IOException, InterruptedException {
            Invariants.requireNonNull(current.key);
            return current;
        }

        @Override
        public boolean nextObject() throws IOException, InterruptedException {
            Invariants.requireNonNull(current.key);
            return current.nextObject();
        }

        @Override
        public Object getObject() throws IOException, InterruptedException {
            Invariants.requireNonNull(current.key);
            return current.getObject();
        }

        private static final class Group<T> implements GroupInfo, ObjectCursor {

            private final Comparator<? super T> comparator;

            T key;

            Supplier<?> values;

            private Object nextValue;

            @SuppressWarnings("unchecked")
            Group(Comparator<? super T> comparator) {
                this.comparator = comparator == null ? (Comparator<? super T>) Comparator.naturalOrder() : comparator;
            }

            @Override
            public Object getValue() throws IOException, InterruptedException {
                return key;
            }

            @Override
            public boolean nextObject() throws IOException, InterruptedException {
                Object next = values.get();
                if (next == null) {
                    nextValue = null;
                    return false;
                } else {
                    nextValue = next;
                    return true;
                }
            }

            @Override
            public Object getObject() throws IOException, InterruptedException {
                if (nextValue == null) {
                    throw new NoSuchElementException();
                }
                return nextValue;
            }

            @SuppressWarnings("unchecked")
            @Override
            public int compareTo(GroupInfo o) {
                return comparator.compare(key, ((Group<T>) o).key);
            }
        }
    }

    static class ObjectWriterConsumer implements ObjectWriter {

        private final Consumer<Object> consumer;

        public ObjectWriterConsumer(Consumer<Object> consumer) {
            Arguments.requireNonNull(consumer);
            this.consumer = consumer;
        }

        @Override
        public void putObject(Object object) throws IOException, InterruptedException {
            consumer.accept(object);
        }
    }
}
