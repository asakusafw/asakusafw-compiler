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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.common.Reportable;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo.Closer;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.vanilla.core.io.DataReader.Provider;
import com.asakusafw.vanilla.core.util.Buffers;
import com.asakusafw.vanilla.core.util.SystemProperty;

/**
 * A basic implementation of {@link BufferPool}.
 * @since 0.4.0
 */
public class BasicBufferPool implements BufferPool, Reportable {

    static final String KEY_PREFIX = "com.asakusafw.vanilla.pool."; //$NON-NLS-1$

    static final String KEY_SMALLER_FIRST = KEY_PREFIX + "swap.smaller";

    static final boolean SMALLER_FIRST = SystemProperty.get(KEY_PREFIX, false);

    static final Logger LOG = LoggerFactory.getLogger(BasicBufferPool.class);

    private static final int DEFAULT_PRIORITY = 0;

    private final long limit;

    private final AtomicLong reserved = new AtomicLong();

    private final NavigableSet<Entry> registered = new ConcurrentSkipListSet<>();

    private final BufferStore store;

    private final Statistics statistics;

    /**
     * Creates a new instance.
     * @param limit the soft limit size of the buffer pool in bytes
     * @param store the buffer store to accept buffers flood from this pool
     */
    public BasicBufferPool(long limit, BufferStore store) {
        Arguments.requireNonNull(store);
        this.limit = limit;
        this.store = store;
        this.statistics = new Statistics(limit);
    }

    @Override
    public long getSize() {
        return reserved.get();
    }

    @Override
    public BufferPool.Ticket reserve(long size) throws IOException, InterruptedException {
        Arguments.require(size >= 0);
        reserved.addAndGet(size);
        try (Closer closer = new Closer()) {
            Ticket t = new Ticket(reserved, size);
            closer.add(t);
            escape();
            closer.keep();
            if (LOG.isDebugEnabled()) {
                statistics.reserved(size);
            }
            return t;
        }
    }

    @Override
    public DataReader.Provider register(BufferPool.Ticket ticket, ByteBuffer buffer) {
        return register(ticket, buffer, DEFAULT_PRIORITY);
    }

    @Override
    public Provider register(BufferPool.Ticket ticket, ByteBuffer buffer, int priority) {
        Arguments.requireNonNull(ticket);
        Arguments.requireNonNull(buffer);
        ByteBuffer b = Buffers.shrink(buffer);
        if (ticket instanceof Ticket) {
            ((Ticket) ticket).shrink(b.capacity());
        }
        Entry entry = new Entry(registered, statistics, b, ticket, priority);
        registered.add(entry);
        if (LOG.isDebugEnabled()) {
            statistics.registerd(ticket.getSize());
        }
        return entry;
    }

    private void escape() throws IOException, InterruptedException {
        while (reserved.get() > limit) {
            Entry next = registered.pollFirst();
            if (next == null) {
                break;
            }
            long size = next.storeTo(store);
            if (LOG.isDebugEnabled()) {
                statistics.stored(size);
            }
        }
        if (LOG.isDebugEnabled()) {
            statistics.total(reserved.get());
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace(toString());
        }
    }

    @Override
    public void report() {
        if (LOG.isDebugEnabled()) {
            statistics.report();
        }
    }

    @Override
    public String toString() {
        return String.format("BufferPool(%,d/%,dbytes)", reserved.get(), limit); //$NON-NLS-1$
    }

    private static final class Ticket implements BufferPool.Ticket {

        private final AtomicLong total;

        private final AtomicLong size;

        Ticket(AtomicLong total, long size) {
            assert size >= 0;
            this.total = total;
            this.size = new AtomicLong(size);
        }

        void shrink(int newSize) {
            while (true) {
                long oldSize = size.get();
                if (newSize >= oldSize) {
                    break;
                }
                if (size.compareAndSet(oldSize, newSize)) {
                    total.addAndGet(newSize - oldSize);
                    break;
                }
            }
        }

        @Override
        public long getSize() {
            return size.get();
        }

        @Override
        public BufferPool.Ticket move() {
            return new Ticket(total, size.getAndSet(0L));
        }

        @Override
        public void close() {
            total.addAndGet(-size.getAndSet(0L));
        }
    }

    private static class Entry implements DataReader.Provider, Comparable<Entry> {

        private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong();

        private final long sequence;

        private final Collection<Entry> queue;

        private final Statistics stats;

        private ByteBuffer buffer;

        private final BufferPool.Ticket ticket;

        private final int size;

        private final int priority;

        private int acquired = 0;

        private boolean deferStore;

        private DataReader.Provider stored;

        Entry(Collection<Entry> queue, Statistics stats, ByteBuffer buffer, BufferPool.Ticket ticket, int priority) {
            this.queue = queue;
            this.stats = stats;
            this.sequence = SEQUENCE_GENERATOR.incrementAndGet();
            this.buffer = Buffers.slice(buffer);
            this.ticket = ticket;
            this.size = buffer.capacity();
            this.priority = priority;
        }

        @Override
        public synchronized DataReader open() throws IOException, InterruptedException {
            if (buffer != null) {
                AtomicBoolean released = new AtomicBoolean();
                acquired++;
                if (LOG.isDebugEnabled()) {
                    stats.readBuffer(size);
                }
                return new ByteBufferReader(Buffers.duplicate(buffer), () -> {
                    if (released.compareAndSet(false, true)) {
                        release();
                    }
                });
            } else if (stored != null) {
                if (LOG.isDebugEnabled()) {
                    stats.readFile(size);
                }
                return stored.open();
            } else {
                throw new IllegalStateException();
            }
        }

        synchronized void release() {
            acquired--;
            // re-activate storeTo()
            if (acquired == 0 && deferStore && buffer != null) {
                deferStore = false;
                queue.add(this);
            }
        }

        synchronized long storeTo(BufferStore store) throws IOException, InterruptedException {
            if (buffer == null) {
                return -1L;
            }
            if (acquired > 0) {
                deferStore = true;
                return -1L;
            }
            Invariants.requireNonNull(buffer);
            Invariants.require(stored == null);
            try (Closer closer = new Closer()) {
                stored = closer.add(store.store(buffer));
                buffer = null;
                ticket.close();
                closer.keep();
                return size;
            }
        }

        @Override
        public int compareTo(Entry o) {
            // higher priority is long lived
            int priorityDiff = Integer.compare(priority, o.priority);
            if (priorityDiff != 0) {
                return priorityDiff;
            }
            // smaller buffer is long lived
            int sizeDiff = SMALLER_FIRST ? Integer.compare(size, o.size) : Integer.compare(o.size, size);
            if (sizeDiff != 0) {
                return sizeDiff;
            }
            // almost entries have different size
            return Long.compare(sequence, o.sequence);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(sequence);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public synchronized void close() throws IOException, InterruptedException {
            try {
                buffer = null;
                if (stored != null) {
                    stored.close();
                    stored = null;
                }
                queue.remove(this);
            } finally {
                ticket.close();
            }
        }
    }

    private static final class Statistics {

        private final long limit;

        private final AtomicLong peak = new AtomicLong();

        private final Item reserved = new Item("buffer reserved"); //$NON-NLS-1$

        private final Item registered = new Item("buffer registered"); //$NON-NLS-1$

        private final Item bufferRead = new Item("buffer read"); //$NON-NLS-1$

        private final Item swapRead = new Item("swap read"); //$NON-NLS-1$

        private final Item swapWrite = new Item("swap write"); //$NON-NLS-1$

        Statistics(long limit) {
            this.limit = limit;
        }

        void total(long size) {
            long current;
            do {
                current = peak.get();
                if (size <= current) {
                    return;
                }
            } while (peak.compareAndSet(current, size) == false);
            peakUpdated(size, current);
        }

        private void peakUpdated(long newPeak, long oldPeak) {
            if (LOG.isTraceEnabled() && newPeak * 2 >= oldPeak) {
                LOG.trace(MessageFormat.format(
                        "peak updated: {0}/{1}bytes",
                        newPeak, limit));
            }
        }

        void reserved(long size) {
            reserved.record(size);
        }

        void registerd(long size) {
            registered.record(size);
        }

        void stored(long size) {
            swapWrite.record(size);
        }

        void readBuffer(long size) {
            bufferRead.record(size);
        }

        void readFile(long size) {
            swapRead.record(size);
        }

        void report() {
            LOG.debug("buffer pool statistics:"); //$NON-NLS-1$
            LOG.debug(MessageFormat.format(
                    "  peak: {0}/{1}bytes", //$NON-NLS-1$
                    peak.get(),
                    limit));
            reserved.show();
            registered.show();
            bufferRead.show();
            swapRead.show();
            swapWrite.show();
        }

        private static final class Item {

            private final String name;

            private final LongAdder count = new LongAdder();

            private final LongAdder total = new LongAdder();

            Item(String name) {
                this.name = name;
            }

            void record(long size) {
                if (size < 0) {
                    return;
                }
                count.increment();
                total.add(size);
            }

            void show() {
                LOG.debug(MessageFormat.format(
                        "  {0}: {1}items, {2}bytes", //$NON-NLS-1$
                        name,
                        count.longValue(),
                        total.longValue()));
            }
        }
    }
}
