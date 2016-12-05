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
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.common.ObjectCursor;
import com.asakusafw.lang.utils.buffer.nio.NioDataBuffer;
import com.asakusafw.lang.utils.buffer.nio.ResizableNioDataBuffer;

/**
 * A {@link ListBuilder} which provides temporary file backed lists.
 * @param <T> the element type
 * @since 0.4.1
 */
public class SpillListBuilder<T> implements ListBuilder<T> {

    private static final int DEFAULT_CACHE_SIZE = 256;

    static final Logger LOG = LoggerFactory.getLogger(SpillListBuilder.class);

    private Entity<T> entity;

    /**
     * Creates a new instance.
     * @param adapter the data adapter
     */
    public SpillListBuilder(DataAdapter<T> adapter) {
        this(adapter, DEFAULT_CACHE_SIZE);
    }

    /**
     * Creates a new instance.
     * @param adapter the data adapter
     * @param cacheSize the number of objects should be cached on Java heap
     */
    public SpillListBuilder(DataAdapter<T> adapter, int cacheSize) {
        this.entity = new Entity<>(adapter, cacheSize);
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

        Entity(DataAdapter<T> adapter, int pageSize) {
            this.store = new Store<>();
            this.adapter = adapter;
            this.elements = (T[]) new Object[pageSize];
            this.currentPageIndex = 0;
            this.sizeInList = 0;
        }

        void reset(ObjectCursor cursor) throws IOException, InterruptedException {
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

        private static final long[] EMPTY_OFFSETS = new long[0];

        Path path;

        FileChannel channel;

        private long[] offsets = EMPTY_OFFSETS;

        private final ResizableNioDataBuffer buffer = new ResizableNioDataBuffer();

        Store() {
            return;
        }

        void putPage(DataAdapter<T> adapter, int index, T[] elements, int count) throws IOException {
            if (path == null) {
                path = Files.createTempFile("spill-", ".bin");
                if (LOG.isDebugEnabled()) {
                    LOG.debug("generating list spill: {}", path);
                }
                channel = FileChannel.open(path,
                        StandardOpenOption.READ, StandardOpenOption.WRITE,
                        StandardOpenOption.DELETE_ON_CLOSE);
                offsets = new long[256];
            }
            if (index + 1 >= offsets.length) {
                offsets = Arrays.copyOf(offsets, offsets.length * 2);
            }
            buffer.contents.clear();
            for (int i = 0; i < count; i++) {
                adapter.write(elements[i], buffer);
            }
            ByteBuffer buf = buffer.contents;
            buf.flip();
            long offset = offsets[index];
            long end = offset + buf.limit();
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("writing list spill: %s#%,d@%,d+%,d", path, index, offset, buf.remaining())); //$NON-NLS-1$
            }
            while (buf.hasRemaining()) {
                offset += channel.write(buf, offset);
            }
            offsets[index + 1] = end;
        }

        void getPage(DataAdapter<T> adapter, int index, T[] elements, int count) throws IOException {
            long offset = offsets[index];
            long end = offsets[index + 1];
            int length = (int) (end - offset);

            ByteBuffer buf = buffer.contents;
            assert length <= buf.capacity();
            buf.clear().limit(length);

            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("reading list spill: %s#%,d@%,d+%,d", path, index, offset, buf.remaining())); //$NON-NLS-1$
            }
            while (buf.hasRemaining()) {
                int read = channel.read(buf, offset);
                if (read < 0) {
                    throw new EOFException();
                }
                offset += read;
            }
            buf.flip();
            for (int i = 0; i < count; i++) {
                adapter.read(buffer, elements[i]);
            }
        }

        @Override
        public void close() throws IOException {
            if (channel != null) {
                offsets = EMPTY_OFFSETS;
                buffer.contents = NioDataBuffer.EMPTY_BUFFER;
                channel.close();
                channel = null;
                path = null;
            }
        }
    }
}
