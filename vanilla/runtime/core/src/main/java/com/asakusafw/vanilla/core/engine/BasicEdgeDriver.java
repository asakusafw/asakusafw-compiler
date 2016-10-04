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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.common.DataComparator;
import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.api.common.Reportable;
import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.api.model.PortId;
import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.vanilla.api.VanillaEdgeDescriptor.Movement;
import com.asakusafw.vanilla.core.io.BasicGroupReader;
import com.asakusafw.vanilla.core.io.BasicKeyValueCursor;
import com.asakusafw.vanilla.core.io.BasicRecordCursor;
import com.asakusafw.vanilla.core.io.BasicRecordSink;
import com.asakusafw.vanilla.core.io.BufferPool;
import com.asakusafw.vanilla.core.io.DataReader;
import com.asakusafw.vanilla.core.io.DataReader.Provider;
import com.asakusafw.vanilla.core.io.DataWriter;
import com.asakusafw.vanilla.core.io.KeyValueCursor;
import com.asakusafw.vanilla.core.io.KeyValueMerger;
import com.asakusafw.vanilla.core.io.KeyValuePartitioner;
import com.asakusafw.vanilla.core.io.RecordCursor;
import com.asakusafw.vanilla.core.io.SharedBuffer;
import com.asakusafw.vanilla.core.io.StreamGroupWriter;
import com.asakusafw.vanilla.core.io.StreamObjectReader;
import com.asakusafw.vanilla.core.io.StreamObjectWriter;
import com.asakusafw.vanilla.core.io.VoidKeyValueCursor;
import com.asakusafw.vanilla.core.mirror.GraphMirror;
import com.asakusafw.vanilla.core.mirror.InputPortMirror;
import com.asakusafw.vanilla.core.mirror.OutputPortMirror;
import com.asakusafw.vanilla.core.mirror.PortMirror;
import com.asakusafw.vanilla.core.mirror.VertexMirror;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * A basic implementation of {@link EdgeDriver}.
 * @since 0.4.0
 */
public class BasicEdgeDriver extends EdgeDriver.Abstract {

    static final Logger LOG = LoggerFactory.getLogger(BasicEdgeDriver.class);

    private final ClassLoader classLoader;

    private final GraphMirror graph;

    private final BufferPool pool;

    private final int numberOfPartitions;

    private final int bufferSizeLimit;

    private final double bufferFlushFactor;

    private final int recordCountLimit;

    private final Map<InputPortMirror, FragmentSource> sources;

    private final Map<OutputPortMirror, FragmentSink> sinks;

    private final Map<InputPortMirror, PartitionedSource> partSources;

    private final Map<OutputPortMirror, PartitionedSink> partSinks;

    /**
     * Creates a new instance.
     * @param classLoader the current class loader
     * @param graph the target graph
     * @param pool the buffer pool
     * @param numberOfPartitions the number of partitions in scatter-gather operations
     * @param bufferSizeLimit each output buffer size threshold in bytes
     * @param bufferFlushFactor the output buffer flush factor
     * @param recordCountLimit the number of limit records in each output buffer
     */
    public BasicEdgeDriver(
            ClassLoader classLoader,
            GraphMirror graph, BufferPool pool,
            int numberOfPartitions,
            int bufferSizeLimit, double bufferFlushFactor, int recordCountLimit) {
        Arguments.requireNonNull(classLoader);
        Arguments.requireNonNull(graph);
        Arguments.requireNonNull(pool);
        Arguments.require(numberOfPartitions > 0);
        Arguments.require(bufferSizeLimit >= 0);
        Arguments.require(recordCountLimit > 0);
        this.classLoader = classLoader;
        this.graph = graph;
        this.pool = pool;
        this.numberOfPartitions = numberOfPartitions;
        this.bufferSizeLimit = bufferSizeLimit;
        this.bufferFlushFactor = bufferFlushFactor;
        this.recordCountLimit = recordCountLimit;
        this.sources = edges(graph, VertexMirror::getInputs, p -> new FragmentSource());
        this.sinks = edges(graph, VertexMirror::getOutputs, p -> new FragmentSink(pool, p.getOpposites().size()));
        this.partSources = parts(graph, VertexMirror::getInputs, p -> new PartitionedSource(numberOfPartitions));
        this.partSinks = parts(graph, VertexMirror::getOutputs,
                p -> new PartitionedSink(pool, numberOfPartitions, p.getOpposites().size()));
    }

    private static <K extends PortMirror, V> Map<K, V> edges(
            GraphMirror graph,
            Function<VertexMirror, Collection<K>> mapper,
            Function<K, V> factory) {
        return graph.getVertices().stream()
                .flatMap(v -> mapper.apply(v).stream())
                .filter(p -> p.getMovement() != Movement.NOTHING
                        && p.getMovement() != Movement.SCATTER_GATHER)
                .collect(Collectors.toConcurrentMap(Function.identity(), factory));
    }

    private static <K extends PortMirror, V> Map<K, V> parts(
            GraphMirror graph,
            Function<VertexMirror, Collection<K>> mapper,
            Function<K, V> factory) {
        return graph.getVertices().stream()
                .flatMap(v -> mapper.apply(v).stream())
                .filter(p -> p.getMovement() == Movement.SCATTER_GATHER)
                .collect(Collectors.toConcurrentMap(Function.identity(), factory));
    }

    @Override
    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }

    @Override
    protected InputPortMirror getInput(PortId id) {
        return graph.getInput(id);
    }

    @Override
    protected OutputPortMirror getOutput(PortId id) {
        return graph.getOutput(id);
    }

    @Override
    protected ObjectReader acquireOneToOneInput(InputPortMirror port) throws IOException, InterruptedException {
        ValueSerDe serde = port.newValueSerDe(classLoader);
        return new StreamObjectReader(
                Invariants.requireNonNull(sources.get(port)).openOneToOne(),
                serde);
    }

    @Override
    protected ObjectWriter acquireOneToOneOutput(OutputPortMirror port) throws IOException, InterruptedException {
        ValueSerDe serde = port.newValueSerDe(classLoader);
        return new StreamObjectWriter(
                BasicRecordSink.stream(Invariants.requireNonNull(sinks.get(port))),
                serde,
                bufferSizeLimit, bufferFlushFactor, recordCountLimit,
                pool.reserve(bufferSizeLimit));
    }

    @Override
    protected ObjectReader acquireBroadcastInput(InputPortMirror port) throws IOException, InterruptedException {
        ValueSerDe serde = port.newValueSerDe(classLoader);
        return new StreamObjectReader(
                Invariants.requireNonNull(sources.get(port)).openBroadcast(),
                serde);
    }

    @Override
    protected ObjectWriter acquireBroadcastOutput(OutputPortMirror port) throws IOException, InterruptedException {
        ValueSerDe serde = port.newValueSerDe(classLoader);
        return new StreamObjectWriter(
                BasicRecordSink.stream(Invariants.requireNonNull(sinks.get(port))),
                serde,
                bufferSizeLimit, bufferFlushFactor, recordCountLimit,
                pool.reserve(bufferSizeLimit));
    }

    @Override
    protected GroupReader acquireScatterGatherInput(
            InputPortMirror port, int taskIndex, int taskCount) throws IOException, InterruptedException {
        Arguments.require(taskCount >= numberOfPartitions);
        KeyValueSerDe serde = port.newKeyValueSerDe(classLoader);
        DataComparator comparator = port.newComparator(classLoader);
        return new BasicGroupReader(
                Invariants.requireNonNull(partSources.get(port)).openScatterGather(comparator, taskIndex),
                serde);
    }

    @Override
    protected ObjectWriter acquireScatterGatherOutput(OutputPortMirror port) throws IOException, InterruptedException {
        KeyValueSerDe serde = port.newKeyValueSerDe(classLoader);
        DataComparator comparator = port.newComparator(classLoader);
        return new StreamGroupWriter(
                KeyValuePartitioner.stream(Arrays.asList(Invariants.requireNonNull(partSinks.get(port)).partitions)),
                serde, comparator,
                bufferSizeLimit, bufferFlushFactor, recordCountLimit,
                pool.reserve(bufferSizeLimit));
    }

    @Override
    protected void completeOneToOneInput(InputPortMirror port) throws IOException, InterruptedException {
        complete(port);
    }

    @Override
    protected void completeOneToOneOutput(OutputPortMirror port) throws IOException, InterruptedException {
        complete(port);
    }

    @Override
    protected void completeBroadcastInput(InputPortMirror port) throws IOException, InterruptedException {
        complete(port);
    }

    @Override
    protected void completeBroadcastOutput(OutputPortMirror port) throws IOException, InterruptedException {
        complete(port);
    }

    private void complete(InputPortMirror port) throws IOException, InterruptedException {
        try (FragmentSource source = sources.remove(port)) {
            Lang.pass(source);
        }
    }

    private void complete(OutputPortMirror port) throws IOException, InterruptedException {
        List<FragmentSource> destinations = port.getOpposites().stream()
                .map(p -> Invariants.requireNonNull(sources.get(p)))
                .collect(Collectors.toList());
        try (FragmentSink sink = sinks.remove(port)) {
            sink.migrateTo(destinations);
        }
    }

    @Override
    protected void completeScatterGatherInput(InputPortMirror port) throws IOException, InterruptedException {
        try (PartitionedSource source = partSources.remove(port)) {
            Lang.pass(source);
        }
    }

    @Override
    protected void completeScatterGatherOutput(OutputPortMirror port) throws IOException, InterruptedException {
        List<PartitionedSource> destinations = port.getOpposites().stream()
                .map(p -> Invariants.requireNonNull(partSources.get(p)))
                .collect(Collectors.toList());
        try (PartitionedSink sink = partSinks.remove(port)) {
            sink.migrateTo(destinations);
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try (Closer closer = new Closer()) {
            sinks.values().forEach(closer::add);
            sinks.clear();
            sources.values().forEach(closer::add);
            sources.clear();
            partSources.values().forEach(closer::add);
            partSources.clear();
            partSinks.values().forEach(closer::add);
            partSinks.clear();
            if (pool instanceof Reportable) {
                ((Reportable) pool).report();
            }
        }
    }

    @Override
    public String toString() {
        return String.format("pool=%,dbytes",
                pool.getSize());
    }

    private static final class FragmentSource implements InterruptibleIo {

        private final Queue<DataReader.Provider> queue = new ConcurrentLinkedQueue<>();

        FragmentSource() {
            return;
        }

        public void offer(DataReader.Provider contents) {
            queue.offer(contents);
        }

        public RecordCursor.Stream openOneToOne() {
            // share chunks
            Queue<DataReader.Provider> q = queue;
            return () -> {
                DataReader.Provider data = q.poll();
                if (data == null) {
                    return null;
                }
                return new InternalRecordCursor(data);
            };
        }

        public RecordCursor.Stream openBroadcast() {
            // repeatable
            Queue<DataReader.Provider> q = new LinkedList<>(queue);
            return () -> {
                DataReader.Provider data = q.poll();
                if (data == null) {
                    return null;
                }
                return BasicRecordCursor.newInstance(data.open());
            };
        }

        public KeyValueCursor openScatterGather(DataComparator comparator) throws IOException, InterruptedException {
            // only once per fragment
            List<DataReader.Provider> all = new ArrayList<>();
            synchronized (this) {
                all.addAll(queue);
                queue.clear();
            }
            List<KeyValueCursor> cursors = new ArrayList<>();
            try (Closer closer = new Closer()) {
                for (DataReader.Provider data : all) {
                    cursors.add(closer.add(new InternalKeyValueCursor(data)));
                }
                closer.keep();
            }
            all.clear();
            switch (cursors.size()) {
            case 0:
                return new VoidKeyValueCursor();
            case 1:
                return cursors.get(0);
            default:
                return new KeyValueMerger(cursors, comparator);
            }
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try (Closer closer = new Closer()) {
                while (true) {
                    Provider next = queue.poll();
                    if (next == null) {
                        break;
                    } else {
                        closer.add(next);
                    }
                }
            }
        }
    }

    private static final class FragmentSink implements InterruptibleIo, DataWriter.Channel {

        private final BufferPool pool;

        private final int priority;

        private final Queue<DataReader.Provider> queue = new ConcurrentLinkedQueue<>();

        FragmentSink(BufferPool pool, int numberOfConsumers) {
            this.pool = pool;
            this.priority = numberOfConsumers;
        }

        @Override
        public DataWriter acquire(int size) throws IOException, InterruptedException {
            Arguments.require(size >= 0);
            return new InternalWriter(pool.reserve(size), Buffers.allocate(size));
        }

        @Override
        public void commit(DataWriter written) throws IOException, InterruptedException {
            Arguments.requireNonNull(written);
            Arguments.require(written instanceof InternalWriter);
            InternalWriter writer = (InternalWriter) written;
            queue.offer(writer.save(pool, priority));
        }

        public void migrateTo(List<FragmentSource> downstreams) {
            while (true) {
                DataReader.Provider next = queue.poll();
                if (next == null) {
                    break;
                }
                List<DataReader.Provider> shared = SharedBuffer.wrap(next, downstreams.size());
                int index = 0;
                for (FragmentSource downstream : downstreams) {
                    downstream.offer(shared.get(index++));
                }
            }
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try (Closer closer = new Closer()) {
                while (true) {
                    Provider next = queue.poll();
                    if (next == null) {
                        break;
                    } else {
                        closer.add(next);
                    }
                }
            }
        }
    }

    private static final class PartitionedSource implements InterruptibleIo {

        final FragmentSource[] partitions;

        PartitionedSource(int numberOfPartitions) {
            this.partitions = Stream.generate(FragmentSource::new)
                    .limit(numberOfPartitions)
                    .toArray(FragmentSource[]::new);
        }

        public KeyValueCursor openScatterGather(
                DataComparator comparator, int taskIndex) throws IOException, InterruptedException {
            if (taskIndex >= partitions.length) {
                return new VoidKeyValueCursor();
            }
            return partitions[taskIndex].openScatterGather(comparator);
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try (Closer closer = new Closer()) {
                Lang.forEach(partitions, closer::add);
            }
        }
    }

    private static final class PartitionedSink implements InterruptibleIo {

        final FragmentSink[] partitions;

        PartitionedSink(BufferPool pool, int numberOfPartitions, int numerOfConsumers) {
            this.partitions = new FragmentSink[numberOfPartitions];
            for (int i = 0; i < partitions.length; i++) {
                partitions[i] = new FragmentSink(pool, numerOfConsumers);
            }
        }

        public void migrateTo(List<PartitionedSource> destinations) {
            PartitionedSource[] dests = destinations.toArray(new PartitionedSource[destinations.size()]);
            FragmentSink[] parts = partitions;
            FragmentSource[][] shuffle = new FragmentSource[parts.length][dests.length];
            for (int pIndex = 0; pIndex < parts.length; pIndex++) {
                for (int dIndex = 0; dIndex < dests.length; dIndex++) {
                    shuffle[pIndex][dIndex] = dests[dIndex].partitions[pIndex];
                }
            }
            for (int i = 0; i < parts.length; i++) {
                parts[i].migrateTo(Arrays.asList(shuffle[i]));
            }
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try (Closer closer = new Closer()) {
                Lang.forEach(partitions, closer::add);
            }
        }
    }

    private static final class InternalRecordCursor implements RecordCursor {

        private final RecordCursor entity;

        private final InterruptibleIo resource;

        InternalRecordCursor(DataReader.Provider data) throws IOException, InterruptedException {
            try (Closer closer = new Closer()) {
                closer.add(data);
                this.entity = BasicRecordCursor.newInstance(data.open());
                resource = closer.move();
            }
        }

        @Override
        public boolean next() throws IOException, InterruptedException {
            return entity.next();
        }

        @Override
        public ByteBuffer get() throws IOException, InterruptedException {
            return entity.get();
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try {
                entity.close();
            } finally {
                resource.close();
            }
        }
    }

    private static final class InternalKeyValueCursor implements KeyValueCursor {

        private final KeyValueCursor entity;

        private final InterruptibleIo resource;

        InternalKeyValueCursor(DataReader.Provider data) throws IOException, InterruptedException {
            try (Closer closer = new Closer()) {
                closer.add(data);
                this.entity = BasicKeyValueCursor.newInstance(data.open());
                resource = closer.move();
            }
        }

        @Override
        public boolean next() throws IOException, InterruptedException {
            return entity.next();
        }

        @Override
        public ByteBuffer getKey() throws IOException, InterruptedException {
            return entity.getKey();
        }

        @Override
        public ByteBuffer getValue() throws IOException, InterruptedException {
            return entity.getValue();
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try {
                entity.close();
            } finally {
                resource.close();
            }
        }
    }

    private static final class InternalWriter implements DataWriter {

        private ByteBuffer buffer;

        private final BufferPool.Ticket ticket;

        InternalWriter(BufferPool.Ticket ticket, ByteBuffer buffer) {
            this.ticket = ticket;
            this.buffer = buffer;
        }

        public DataReader.Provider save(BufferPool pool, int priority) throws IOException, InterruptedException {
            ByteBuffer b = buffer;
            Invariants.requireNonNull(b);
            buffer = null;
            b = Buffers.duplicate(b);
            b.flip();
            DataReader.Provider result = pool.register(ticket.move(), b, priority);
            return result;
        }

        @Override
        public ByteBuffer getBuffer() {
            Invariants.requireNonNull(buffer);
            return buffer;
        }

        @Override
        public void writeInt(int value) {
            Invariants.requireNonNull(buffer);
            buffer.putInt(value);
        }

        @Override
        public void writeFully(ByteBuffer source) {
            Invariants.requireNonNull(buffer);
            buffer.put(source);
        }

        @Override
        public void close() throws InterruptedException, IOException {
            buffer = null;
            ticket.close();
        }
    }
}
