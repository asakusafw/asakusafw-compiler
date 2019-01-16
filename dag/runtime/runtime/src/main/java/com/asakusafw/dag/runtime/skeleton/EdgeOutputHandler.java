/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.EdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.adapter.ObjectCombiner;
import com.asakusafw.dag.runtime.adapter.ObjectCopier;
import com.asakusafw.dag.runtime.adapter.OutputHandler;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.runtime.core.Result;

/**
 * Handles edge outputs for vertices.
 * @since 0.4.0
 * @version 0.4.1
 */
final class EdgeOutputHandler implements OutputHandler<EdgeIoProcessorContext> {

    static final Logger LOG = LoggerFactory.getLogger(EdgeOutputHandler.class);

    private final Map<String, Sink> sinks;

    EdgeOutputHandler(Collection<OutputSpec> names) {
        Arguments.requireNonNull(names);
        Arguments.require(names.isEmpty() == false);
        this.sinks = names.stream()
                .collect(Collectors.toMap(s -> s.name, s -> s.toSink()));
    }

    @Override
    public boolean contains(String id) {
        Arguments.requireNonNull(id);
        return sinks.containsKey(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Result<T> getSink(Class<T> outputType, String name) {
        Invariants.require(sinks.containsKey(name));
        return (Result<T>) sinks.get(name);
    }

    @Override
    public Session start(EdgeIoProcessorContext context) throws IOException, InterruptedException {
        for (Sink entry : sinks.values()) {
            entry.connect(context);
        }
        return this::closeSession;
    }

    void closeSession() throws IOException, InterruptedException {
        for (Sink entry : sinks.values()) {
            entry.disconnect();
        }
    }

    private interface Sink extends Result<Object> {

        void connect(EdgeIoProcessorContext context) throws IOException, InterruptedException;

        void disconnect() throws IOException, InterruptedException;
    }

    private static final class SimpleSink implements Sink {

        private final String name;

        private ObjectWriter writer;

        SimpleSink(String name) {
            this.name = name;
        }

        @Override
        public void connect(EdgeIoProcessorContext context) throws IOException, InterruptedException {
            disconnect();
            writer = (ObjectWriter) context.getOutput(name);
        }

        @Override
        public void disconnect() throws IOException, InterruptedException {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }

        @Override
        public void add(Object result) {
            ObjectWriter w = writer;
            if (w == null) {
                throw new IllegalStateException(MessageFormat.format(
                        "output \"{0}\" is not ready", //$NON-NLS-1$
                        name));
            }
            try {
                w.putObject(result);
            } catch (IOException | InterruptedException e) {
                throw new Result.OutputException(e);
            }
        }
    }

    private static final class MappingSink implements Sink {

        private final Function<Object, Object> mapper;

        private final Sink delegate;

        @SuppressWarnings("unchecked")
        MappingSink(Sink delegate, Function<?, ?> mapper) {
            this.delegate = delegate;
            this.mapper = (Function<Object, Object>) mapper;
        }

        @Override
        public void connect(EdgeIoProcessorContext context) throws IOException, InterruptedException {
            delegate.connect(context);
        }

        @Override
        public void disconnect() throws IOException, InterruptedException {
            delegate.disconnect();
        }

        @Override
        public void add(Object result) {
            delegate.add(mapper.apply(result));
        }
    }

    private static final class MapAggregateSink implements Sink {

        private final String name;

        private final Sink delegate;

        private final ObjectCopier<Object> copier;

        private final ObjectCombiner<Object> combiner;

        private final KeyBuffer key;

        private final Map<KeyBuffer.View, Object> table;

        private final int tableSize;

        private final Object[] recycleTable;

        private int recycleTop;

        private long countCombine;

        private long countFlush;

        @SuppressWarnings("unchecked")
        MapAggregateSink(
                String name, Sink delegate,
                ObjectCopier<?> copier, ObjectCombiner<?> combiner,
                KeyBuffer keyBuffer, int tableSize) {
            this.name = name;
            this.delegate = delegate;
            this.copier = (ObjectCopier<Object>) copier;
            this.combiner = (ObjectCombiner<Object>) combiner;
            this.key = keyBuffer;
            this.tableSize = tableSize;
            this.table = new HashMap<>(tableSize * 2, 0.75f);
            this.recycleTable = new Object[tableSize];
            this.recycleTop = -1;
        }

        @Override
        public void connect(EdgeIoProcessorContext context) throws IOException, InterruptedException {
            Invariants.require(table.isEmpty());
            countCombine = 0;
            countFlush = 0;
            delegate.connect(context);
        }

        @Override
        public void disconnect() throws IOException, InterruptedException {
            flush();
            if (LOG.isDebugEnabled()) {
                LOG.debug("pre-aggregation stats at {}: {}/{}", name, countCombine, countCombine + countFlush);
            }
            delegate.disconnect();
        }

        @Override
        public void add(Object result) {
            key.clear();
            combiner.buildKey(key, result);
            Object left = table.get(key.getView());
            if (left == null) {
                if (table.size() >= tableSize) {
                    flush();
                }
                table.put(key.getFrozen(), copy(result));
            } else {
                countCombine++;
                combiner.combine(left, result);
            }
        }

        private Object copy(Object result) {
            int ri = recycleTop;
            if (ri >= 0) {
                Object[] rs = recycleTable;
                Object recycle = rs[ri];
                rs[ri] = null;
                recycleTop = ri - 1;
                return copier.newCopy(result, recycle);
            }
            return copier.newCopy(result);
        }

        private void flush() {
            if (LOG.isTraceEnabled()) {
                for (Object value : table.values()) {
                    LOG.trace("pre-aggregation object at {}: {}", name, value);
                }
            }
            int index = recycleTop + 1;
            Object[] recycles = recycleTable;
            for (Object value : table.values()) {
                delegate.add(value);
                if (index < recycles.length) {
                    recycles[index++] = value;
                }
            }
            countFlush += table.size();
            table.clear();
            recycleTop = index - 1;
        }
    }

    private static final class HashAggregateSink implements Sink {

        private final String name;

        private final Sink delegate;

        private final ObjectCopier<Object> copier;

        private final ObjectCombiner<Object> combiner;

        private final KeyBuffer workKey;

        private final KeyBuffer[] keyTable;

        private final Object[] valueTable;

        private final int tableSize;

        private long countCombine;

        private long countFlush;

        @SuppressWarnings("unchecked")
        HashAggregateSink(
                String name, Sink delegate,
                ObjectCopier<?> copier, ObjectCombiner<?> combiner,
                Supplier<? extends KeyBuffer> keyBufferSupplier,
                int tableSize) {
            this.name = name;
            this.delegate = delegate;
            this.copier = (ObjectCopier<Object>) copier;
            this.combiner = (ObjectCombiner<Object>) combiner;
            this.workKey = keyBufferSupplier.get();
            this.keyTable = new KeyBuffer[tableSize];
            this.valueTable = new Object[tableSize];
            this.tableSize = tableSize;
            for (int i = 0; i < tableSize; i++) {
                keyTable[i] = keyBufferSupplier.get();
            }
        }

        @Override
        public void connect(EdgeIoProcessorContext context) throws IOException, InterruptedException {
            countCombine = 0;
            countFlush = 0;
            delegate.connect(context);
        }

        @Override
        public void disconnect() throws IOException, InterruptedException {
            flush();
            if (LOG.isDebugEnabled()) {
                LOG.debug("pre-aggregation stats at {}: {}/{}", name, countCombine, countCombine + countFlush);
            }
            Arrays.fill(valueTable, null);
            delegate.disconnect();
        }

        @Override
        public void add(Object result) {
            workKey.clear();
            combiner.buildKey(workKey, result);
            int index = (workKey.hashCode() & Integer.MAX_VALUE) % tableSize;
            KeyBuffer k = keyTable[index];
            Object[] values = valueTable;
            if (values[index] == null) {
                combiner.buildKey(k.clear(), result);
                values[index] = copier.newCopy(result);
            } else if (k.equals(workKey)) {
                countCombine++;
                combiner.combine(values[index], result);
            } else {
                Object v = values[index];
                doAdd(v);
                combiner.buildKey(k.clear(), result);
                values[index] = copier.newCopy(result, v);
            }
        }

        private void flush() {
            Object[] values = valueTable;
            for (int i = 0; i < values.length; i++) {
                Object v = values[i];
                if (v != null) {
                    doAdd(v);
                    values[i] = null;
                }
            }
        }

        private void doAdd(Object value) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("pre-aggregation object at {}: {}", name, value);
            }
            countFlush++;
            delegate.add(value);
        }
    }

    static class OutputSpec {

        final String name;

        final Supplier<? extends Function<?, ?>> mapperSupplier;

        final Supplier<? extends ObjectCopier<?>> copierSupplier;

        final Supplier<? extends ObjectCombiner<?>> combinerSupplier;

        final int tableSize;

        final Supplier<? extends KeyBuffer> keyBufferSupplier;

        final AggregationStrategy aggregationStrategy;

        OutputSpec(String name) {
            this(name, null, null, null, null, -1, AggregationStrategy.DISABLED);
        }

        OutputSpec(
                String name,
                Supplier<? extends Function<?, ?>> mapperSupplier,
                Supplier<? extends ObjectCopier<?>> copierSupplier,
                Supplier<? extends ObjectCombiner<?>> combinerSupplier,
                Supplier<? extends KeyBuffer> keyBufferSupplier,
                int tableSize,
                AggregationStrategy aggregationStrategy) {
            Arguments.requireNonNull(name);
            this.name = name;
            this.mapperSupplier = mapperSupplier;
            this.copierSupplier = copierSupplier;
            this.combinerSupplier = combinerSupplier;
            this.keyBufferSupplier = keyBufferSupplier;
            this.tableSize = tableSize;
            this.aggregationStrategy = aggregationStrategy;
        }

        Sink toSink() {
            Sink result = new SimpleSink(name);
            if (aggregationStrategy != AggregationStrategy.DISABLED) {
                Invariants.requireNonNull(copierSupplier);
                Invariants.requireNonNull(combinerSupplier);
                Invariants.requireNonNull(keyBufferSupplier);
                Invariants.require(tableSize >= 1);
                switch (aggregationStrategy) {
                case MAP:
                    result = new MapAggregateSink(
                            name, result,
                            copierSupplier.get(), combinerSupplier.get(),
                            keyBufferSupplier.get(), tableSize);
                    break;
                case HASH:
                    result = new HashAggregateSink(
                            name, result,
                            copierSupplier.get(), combinerSupplier.get(),
                            keyBufferSupplier, tableSize);
                    break;
                default:
                    throw new AssertionError(aggregationStrategy);
                }
            }
            if (mapperSupplier != null) {
                result = new MappingSink(result, mapperSupplier.get());
            }
            return result;
        }
    }

    /**
     * Represents a kind of aggregation strategy in output edge.
     * @since 0.4.1
     */
    enum AggregationStrategy {

        /**
         * Aggregation is disabled.
         */
        DISABLED,

        /**
         * Aggregate on Java hash map (the default implementation).
         * The sink will flush entries only if the number of aggregating entries is greater than the table size.
         */
        MAP,

        /**
         * Aggregate on simple hash table.
         * If key-hash contention was occurred, the sink will flush the elder entry.
         */
        HASH,
    }
}
