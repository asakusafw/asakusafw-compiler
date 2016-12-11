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
package com.asakusafw.dag.runtime.data;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.common.ObjectCursor;
import com.asakusafw.lang.utils.buffer.nio.ResizableNioDataBuffer;

/**
 * A {@link ListBuilder} which provides temporary file backed lists.
 * @param <T> the element type
 * @since 0.4.1
 */
public class SpillListBuilder<T> implements ListBuilder<T> {

    private static final int DEFAULT_CACHE_SIZE = 256;

    private static final int DEFAULT_BUFFER_SOFT_LIMIT = 4 * 1024 * 1024;

    static final Logger LOG = LoggerFactory.getLogger(SpillListBuilder.class);

    private Entity<T> entity;

    /**
     * Creates a new instance.
     * @param adapter the data adapter
     */
    public SpillListBuilder(DataAdapter<T> adapter) {
        this(adapter, DEFAULT_CACHE_SIZE, DEFAULT_BUFFER_SOFT_LIMIT);
    }

    /**
     * Creates a new instance.
     * @param adapter the data adapter
     * @param cacheSize the number of objects should be cached on Java heap
     */
    public SpillListBuilder(DataAdapter<T> adapter, int cacheSize) {
        this.entity = new Entity<>(adapter, cacheSize, DEFAULT_BUFFER_SOFT_LIMIT);
    }

    /**
     * Creates a new instance.
     * @param adapter the data adapter
     * @param cacheSize the number of objects should be cached on Java heap
     * @param bufferSoftLimit the buffer size soft limit in bytes
     */
    public SpillListBuilder(DataAdapter<T> adapter, int cacheSize, int bufferSoftLimit) {
        this.entity = new Entity<>(adapter, cacheSize, bufferSoftLimit);
    }

    @Override
    public List<T> build(ObjectCursor cursor) throws IOException, InterruptedException {
        entity.reset(cursor);
        return entity;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        Arrays.fill(entity.elements, null);
        entity.store.close();
    }

    @SuppressWarnings("unchecked")
    private static class Entity<T> extends AbstractList<T> {

        final DataAdapter<T> adapter;

        final Store<T> store;

        final T[] elements;

        int currentPageIndex;

        int sizeInList;

        Entity(DataAdapter<T> adapter, int pageSize, int bufferSoftLimit) {
            this.store = new Store<>(bufferSoftLimit);
            this.adapter = adapter;
            this.elements = (T[]) new Object[pageSize];
            this.currentPageIndex = 0;
            this.sizeInList = 0;
        }

        void reset(ObjectCursor cursor) throws IOException, InterruptedException {
            store.reset();
            DataAdapter<T> da = adapter;
            T[] es = this.elements;
            int offset = 0;
            int pageOffset = 0;
            while (cursor.nextObject()) {
                if (offset >= es.length) {
                    // escape into buffer
                    store.putPage(da, pageOffset, es, offset);
                    offset = 0;
                    pageOffset++;
                }
                T object = (T) cursor.getObject();
                T destination = es[offset];
                if (destination == null) {
                    destination = da.create();
                    es[offset] = destination;
                }
                da.copy(object, destination);
                offset++;
            }
            if (pageOffset != 0) {
                assert offset > 0;
                store.putPage(da, pageOffset, es, offset);
            }
            sizeInList = pageOffset * es.length + offset;
            currentPageIndex = pageOffset;
        }

        @Override
        public T get(int index) {
            if (index < 0 || index >= sizeInList) {
                throw new IndexOutOfBoundsException();
            }
            int windowSize = elements.length;
            int pageIndex = index / windowSize;
            int offsetInPage = index % windowSize;
            if (currentPageIndex != pageIndex) {
                int count = Math.min(sizeInList - pageIndex * windowSize, windowSize);
                try {
                    store.getPage(adapter, pageIndex, elements, count);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                currentPageIndex = pageIndex;
            }
            return elements[offsetInPage];
        }

        @Override
        public int size() {
            return sizeInList;
        }
    }

    private static class Store<T> implements Closeable {

        private static final int[] EMPTY_INTS = new int[0];

        private static final long[] EMPTY_LONGS = new long[0];

        private final int bufferSoftLimit;

        private Path path;

        private FileChannel channel;

        private long[] offsets = EMPTY_LONGS;

        private long[] fragmentEndOffsets = EMPTY_LONGS;

        private int[] fragmentElementCounts = EMPTY_INTS;

        private int fragmentTableLimit;

        private final ResizableNioDataBuffer buffer = new ResizableNioDataBuffer();

        Store(int bufferSoftLimit) {
            this.bufferSoftLimit = bufferSoftLimit;
        }

        void reset() {
            this.fragmentTableLimit = 0;
        }

        void putPage(DataAdapter<T> adapter, int index, T[] elements, int count) throws IOException {
            if (path == null) {
                path = Files.createTempFile("spill-", ".bin");
                if (LOG.isDebugEnabled()) {
                    LOG.debug("generating list spill: {}", path);
                }
                channel = FileChannel.open(path,
                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE,
                        StandardOpenOption.DELETE_ON_CLOSE);
                offsets = new long[256];
            }
            if (index >= offsets.length) {
                offsets = Arrays.copyOf(offsets, offsets.length * 2);
            }
            int fragmentBegin = 0;
            long offset = index == 0 ? 0L : offsets[index - 1];
            buffer.contents.clear();
            for (int i = 0; i < count; i++) {
                if (buffer.contents.position() > bufferSoftLimit) {
                    // write fragment if buffer was exceeded
                    int fragmentEnd = i;
                    assert fragmentEnd > fragmentBegin;
                    offset = putFragment(offset, fragmentEnd - fragmentBegin, buffer.contents);
                    buffer.contents.clear();
                    fragmentBegin = fragmentEnd;
                }
                adapter.write(elements[i], buffer);
            }
            assert buffer.contents.hasRemaining();
            long end = putContents(offset, buffer.contents);
            offsets[index] = end;
        }

        private long putFragment(long begin, int elementCount, ByteBuffer contents) throws IOException {
            assert elementCount > 0;
            if (fragmentTableLimit >= fragmentEndOffsets.length) {
                int size = Math.max(fragmentEndOffsets.length * 2, 256);
                fragmentEndOffsets = Arrays.copyOf(fragmentEndOffsets, size);
                fragmentElementCounts = Arrays.copyOf(fragmentElementCounts, size);
            }
            long end = putContents(begin, contents);
            fragmentEndOffsets[fragmentTableLimit] = end;
            fragmentElementCounts[fragmentTableLimit] = elementCount;
            fragmentTableLimit++;
            return end;
        }

        private long putContents(long begin, ByteBuffer contents) throws IOException {
            contents.flip();
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("writing page fragment: %s@%,d+%,d", path, begin, contents.remaining())); //$NON-NLS-1$
            }
            long offset = begin;
            while (contents.hasRemaining()) {
                offset += channel.write(contents, offset);
            }
            return offset;
        }

        void getPage(DataAdapter<T> adapter, int index, T[] elements, int count) throws IOException {
            long offset = index == 0 ? 0L : offsets[index - 1];
            long end = offsets[index];
            long length = end - offset;
            ByteBuffer buf = buffer.contents;
            if (buf.capacity() >= length) {
                readFragment(adapter, offset, end, elements, 0, count);
            } else {
                getPageFragments(adapter, offset, end, elements, count);
            }
        }

        private void getPageFragments(
                DataAdapter<T> adapter,
                long begin, long end,
                T[] elements, int count) throws IOException {
            long fileOffset = begin;
            int arrayOffset = 0;
            int fIndex = findFragmentsIndex(begin, end);
            for (int i = fIndex, n = fragmentTableLimit; i < n; i++) {
                long fragmentEnd = fragmentEndOffsets[i];
                if (fragmentEnd >= end) {
                    break;
                }
                int arrayEnd = arrayOffset + fragmentElementCounts[i];
                // reads rest elements
                readFragment(adapter, fileOffset, fragmentEnd, elements, arrayOffset, arrayEnd);
                fileOffset = fragmentEnd;
                arrayOffset = arrayEnd;
            }
            assert fileOffset < end;
            assert arrayOffset < count;
            // reads rest elements
            readFragment(adapter, fileOffset, end, elements, arrayOffset, count);
        }

        private int findFragmentsIndex(long begin, long end) {
            int fIndex = Arrays.binarySearch(fragmentEndOffsets, 0, fragmentTableLimit, begin);
            if (fIndex == fragmentTableLimit || fIndex >= 0) {
                throw new IllegalStateException();
            }
            fIndex = -(fIndex + 1);
            assert begin < fragmentEndOffsets[fIndex] && fragmentEndOffsets[fIndex] < end;
            return fIndex;
        }

        private void readFragment(
                DataAdapter<T> adapter,
                long fileBegin, long fileEnd,
                T[] elements, int arrayBegin, int arrayEnd) throws IOException {
            ByteBuffer buf = buffer.contents;
            int fileSize = (int) (fileEnd - fileBegin);
            buf.clear().limit(fileSize);
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("reading page fragment: %s@%,d+%,d", path, fileBegin, buf.remaining())); //$NON-NLS-1$
            }
            long offset = fileBegin;
            while (buf.hasRemaining()) {
                int read = channel.read(buf, offset);
                if (read < 0) {
                    throw new IllegalStateException();
                }
                offset += read;
            }
            buf.flip();
            for (int i = arrayBegin; i < arrayEnd; i++) {
                adapter.read(buffer, elements[i]);
            }
        }

        @Override
        public void close() throws IOException {
            if (channel != null) {
                offsets = EMPTY_LONGS;
                fragmentEndOffsets = EMPTY_LONGS;
                fragmentElementCounts = EMPTY_INTS;
                buffer.contents = ResizableNioDataBuffer.EMPTY_BUFFER;
                channel.close(); // DELETE_ON_CLOSE
                if (Files.exists(path) && Files.deleteIfExists(path) == false && Files.exists(path)) {
                    LOG.warn(MessageFormat.format(
                            "failed to delete a temporary file: {0}",
                            path));
                }
                channel = null;
                path = null;
            }
        }
    }
}
