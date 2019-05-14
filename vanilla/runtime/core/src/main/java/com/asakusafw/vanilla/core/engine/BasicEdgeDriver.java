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
package com.asakusafw.vanilla.core.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.common.DataComparator;
import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.api.common.Reportable;
import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.api.model.PortId;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor.Movement;
import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.vanilla.core.io.BasicGroupReader;
import com.asakusafw.vanilla.core.io.BasicKeyValueCursor;
import com.asakusafw.vanilla.core.io.BasicKeyValueSink;
import com.asakusafw.vanilla.core.io.BasicRecordCursor;
import com.asakusafw.vanilla.core.io.BasicRecordSink;
import com.asakusafw.vanilla.core.io.BlobStore;
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
 * @version 0.5.3
 */
public class BasicEdgeDriver extends EdgeDriver.Abstract {

    static final Logger LOG = LoggerFactory.getLogger(BasicEdgeDriver.class);

    private final ClassLoader classLoader;

    private final GraphMirror graph;

    private final BufferPool pool;

    private final int numberOfPartitions;

    private final int bufferSizeLimit;

    private final int bufferMarginSize;

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
     * @param blobs the BLOB store
     * @param numberOfPartitions the number of partitions in scatter-gather operations
     * @param bufferSizeLimit each output buffer size threshold in bytes
     * @param bufferMarginSize the output buffer margin size
     * @param recordCountLimit the number of limit records in each output buffer
     * @param mergeThreshold the maximum number of merging scatter/gather input chunks
     * @param mergeFactor the fraction to merge scatter/gather input with {@code mergeThreshold}
     */
    public BasicEdgeDriver(
            ClassLoader classLoader,
            GraphMirror graph, BufferPool pool, BlobStore blobs,
            int numberOfPartitions,
            int bufferSizeLimit, int bufferMarginSize, int recordCountLimit,
            int mergeThreshold, double mergeFactor) {
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
        this.bufferMarginSize = bufferMarginSize;
        this.recordCountLimit = recordCountLimit;
        int mergeCount = Math.max(2, Math.min(mergeThreshold, (int) (mergeThreshold * mergeFactor)));
        Function<PortMirror, Supplier<FragmentStore>> fstore =
                p -> () -> new FragmentStore(blobs, p.newComparator(classLoader), mergeThreshold, mergeCount);
        this.sources = edges(graph, VertexMirror::getInputs,
                p -> new FragmentSource());
        this.sinks = edges(graph, VertexMirror::getOutputs,
                p -> new FragmentSink(pool, p.getOpposites().size()));
        this.partSources = parts(graph, VertexMirror::getInputs,
                p -> new PartitionedSource(numberOfPartitions, fstore.apply(p)));
        this.partSinks = parts(graph, VertexMirror::getOutputs,
                p -> new PartitionedSink(pool, numberOfPartitions, p.getOpposites().size(), fstore.apply(p)));
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
                bufferSizeLimit, bufferMarginSize, recordCountLimit,
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
                bufferSizeLimit, bufferMarginSize, recordCountLimit,
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
                bufferSizeLimit, bufferMarginSize, recordCountLimit,
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

        private final FragmentStore store;

        FragmentSource() {
            this(new FragmentStore());
        }

        FragmentSource(FragmentStore store) {
            this.store = store;
        }

        public void offer(Fragment fragment) throws IOException, InterruptedException {
            store.offer(fragment);
        }

        public RecordCursor.Stream openOneToOne() {
            // share chunks
            FragmentStore s = store;
            return () -> {
                Fragment fragment = s.poll();
                if (fragment == null) {
                    return null;
                }
                return new InternalRecordCursor(fragment.source);
            };
        }

        public RecordCursor.Stream openBroadcast() {
            // repeatable
            Queue<Fragment> s = store.entries();
            return () -> {
                Fragment fragment = s.poll();
                if (fragment == null) {
                    return null;
                }
                return BasicRecordCursor.newInstance(fragment.source.open());
            };
        }

        public KeyValueCursor openScatterGather(DataComparator comparator) throws IOException, InterruptedException {
            // only once per fragment
            List<KeyValueCursor> cursors = new ArrayList<>();
            long size = 0;
            try (Closer closer = new Closer()) {
                while (true) {
                    Fragment fragment = store.poll();
                    if (fragment == null) {
                        break;
                    }
                    size += fragment.size;
                    cursors.add(closer.add(new InternalKeyValueCursor(fragment.source)));
                }
                closer.keep();
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("opening scatter/gather {} input fragments: {}bytes",
                        cursors.size(),
                        size);
            }
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
            store.close();
        }
    }

    private static final class FragmentSink implements InterruptibleIo, DataWriter.Channel {

        private final BufferPool pool;

        private final int priority;

        private final FragmentStore store;

        FragmentSink(BufferPool pool, int numberOfConsumers) {
            this(pool, numberOfConsumers, new FragmentStore());
        }

        FragmentSink(BufferPool pool, int numberOfConsumers, FragmentStore store) {
            this.pool = pool;
            this.priority = numberOfConsumers;
            this.store = store;
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
            store.offer(writer.save(pool, priority));
        }

        public void migrateTo(List<FragmentSource> downstreams) throws IOException, InterruptedException {
            while (true) {
                Fragment next = store.poll();
                if (next == null) {
                    break;
                }
                List<DataReader.Provider> shared = SharedBuffer.wrap(next.source, downstreams.size());
                int index = 0;
                for (FragmentSource downstream : downstreams) {
                    downstream.offer(new Fragment(shared.get(index++), next.size));
                }
            }
        }

        @Override
        public void close() throws IOException, InterruptedException {
            store.close();
        }
    }

    private static final class FragmentStore implements InterruptibleIo {

        private final BlobStore blobs;

        private final DataComparator comparator;

        private final int mergeThreshold;

        private final int mergeCount;

        private final Queue<Fragment> queue = new ConcurrentLinkedQueue<>();

        private final AtomicInteger count = new AtomicInteger();

        FragmentStore() {
            this(null, null, 0, 0);
        }

        FragmentStore(BlobStore blobs, DataComparator comparator, int mergeThreshold, int mergeCount) {
            this.blobs = blobs;
            this.comparator = comparator;
            this.mergeThreshold = mergeThreshold;
            this.mergeCount = mergeCount;
        }

        void offer(Fragment fragment) throws IOException, InterruptedException {
            queue.offer(fragment);
            count.incrementAndGet();
            merge();
        }

        Fragment poll() {
            Fragment result = queue.poll();
            if (result == null) {
                return null;
            }
            count.decrementAndGet();
            return result;
        }

        Queue<Fragment> entries() {
            return new ArrayDeque<>(queue);
        }

        private void merge() throws IOException, InterruptedException {
            while (mergeThreshold > 1 && count.get() > mergeThreshold) {
                ArrayList<Fragment> fragments;
                synchronized (this) {
                    if (queue.size() <= mergeThreshold) {
                        return;
                    }
                    fragments = new ArrayList<>();
                    while (true) {
                        Fragment fragment = queue.poll();
                        if (fragment == null) {
                            break;
                        }
                        fragments.add(fragment);
                        count.decrementAndGet();
                    }
                }
                fragments.sort(Comparator.comparing((Fragment f) -> f.size));
                int mergeTargets = Math.min(fragments.size(), mergeCount);
                for (int i = mergeTargets, n = fragments.size(); i < n; i++) {
                    queue.offer(fragments.remove(fragments.size() - 1));
                    count.incrementAndGet();
                }
                Fragment merged = doMerge(fragments);
                queue.offer(merged);
                count.incrementAndGet();
            }
        }

        private Fragment doMerge(List<Fragment> fragments) throws IOException, InterruptedException {
            List<KeyValueCursor> cursors = new ArrayList<>(fragments.size());
            try (Closer closer = new Closer()) {
                for (Fragment fragment : fragments) {
                    cursors.add(closer.add(new InternalKeyValueCursor(fragment.source)));
                }
                closer.keep();
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("start merging scatter/gather input chunks: count={}, size={}",
                        fragments.size(),
                        fragments.stream().mapToLong(it -> it.size).sum());
            }
            try (KeyValueMerger merger = new KeyValueMerger(cursors, comparator);
                    DataWriter writer = blobs.create()) {
                long size = BasicKeyValueSink.copy(merger, writer);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("merged scatter/gather {} input fragments: {}bytes->{}bytes",
                            fragments.size(),
                            fragments.stream().mapToLong(it -> it.size).sum(),
                            size);
                }
                return new Fragment(blobs.commit(writer), size);
            }
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try (Closer closer = new Closer()) {
                while (true) {
                    Fragment next = queue.poll();
                    if (next == null) {
                        break;
                    } else {
                        closer.add(next);
                    }
                }
            }
        }
    }

    private static final class Fragment implements InterruptibleIo {

        final DataReader.Provider source;

        long size;

        Fragment(Provider source, long size) {
            this.source = source;
            this.size = size;
        }

        @Override
        public void close() throws IOException, InterruptedException {
            source.close();
        }
    }

    private static final class PartitionedSource implements InterruptibleIo {

        final FragmentSource[] partitions;

        PartitionedSource(int numberOfPartitions, Supplier<? extends FragmentStore> store) {
            this.partitions = Stream.generate(() -> new FragmentSource(store.get()))
                    .limit(numberOfPartitions)
                    .toArray(FragmentSource[]::new);
        }

        KeyValueCursor openScatterGather(
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

        PartitionedSink(
                BufferPool pool, int numberOfPartitions,
                int numerOfConsumers, Supplier<? extends FragmentStore> stores) {
            this.partitions = new FragmentSink[numberOfPartitions];
            for (int i = 0; i < partitions.length; i++) {
                partitions[i] = new FragmentSink(pool, numerOfConsumers, stores.get());
            }
        }

        void migrateTo(List<PartitionedSource> destinations) throws IOException, InterruptedException {
            FragmentSink[] parts = partitions;
            for (int pIndex = 0; pIndex < parts.length; pIndex++) {
                List<FragmentSource> shuffle = collectDestinations(destinations, pIndex);
                parts[pIndex].migrateTo(shuffle);
            }
        }

        private List<FragmentSource> collectDestinations(List<PartitionedSource> destinations, int partitionId) {
            return destinations.stream()
                    .map(it -> it.partitions[partitionId])
                    .collect(Collectors.toList());
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

        Fragment save(BufferPool pool, int priority) throws IOException, InterruptedException {
            ByteBuffer b = buffer;
            Invariants.requireNonNull(b);
            buffer = null;
            b = Buffers.duplicate(b);
            b.flip();
            long size = b.remaining();
            DataReader.Provider result = pool.register(ticket.move(), b, priority);
            return new Fragment(result, size);
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
