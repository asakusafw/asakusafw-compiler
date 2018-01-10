/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import com.asakusafw.dag.api.common.Serializer;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.vanilla.core.util.ExtensibleDataBuffer;

/**
 * An implementation of {@link ObjectWriter} using {@link RecordSink}.
 * @since 0.4.0
 */
public class StreamObjectWriter implements ObjectWriter {

    private static final int[] EMPTY = new int[0];

    private final RecordSink.Stream sinks;

    private final Serializer serializer;

    private final int bufferSizeSoftLimit;

    private final int recordCountLimit;

    private final ExtensibleDataBuffer buffer;

    private int[] sizes = EMPTY;

    private int recordCount = 0;

    private final InterruptibleIo resource;

    /**
     * Creates a new instance.
     * @param sinks the next sink provider
     * @param serializer the object serializer
     * @param bufferSizeLimit the internal buffer size limit in bytes
     * @param recordCountLimit the number of limit records in each page
     */
    public StreamObjectWriter(
            RecordSink.Stream sinks,
            Serializer serializer,
            int bufferSizeLimit, int recordCountLimit) {
        this(sinks, serializer,
                bufferSizeLimit, Util.DEFAULT_BUFFER_FLUSH_FACTOR, recordCountLimit,
                null);
    }

    /**
     * Creates a new instance.
     * @param sinks the next sink provider
     * @param serializer the object serializer
     * @param bufferSizeLimit the internal buffer size limit in bytes
     * @param bufferFlushFactor the internal buffer flush factor in the limit
     * @param recordCountLimit the number of limit records in each page
     * @param resource the attached resource (nullable)
     */
    public StreamObjectWriter(
            RecordSink.Stream sinks,
            Serializer serializer,
            int bufferSizeLimit, double bufferFlushFactor, int recordCountLimit,
            InterruptibleIo resource) {
        Arguments.requireNonNull(sinks);
        Arguments.requireNonNull(serializer);
        Arguments.require(bufferSizeLimit > 0);
        Arguments.require(recordCountLimit > 0);
        this.sinks = sinks;
        this.serializer = serializer;
        this.bufferSizeSoftLimit = Util.getBufferThreshold(bufferSizeLimit, bufferFlushFactor);
        this.recordCountLimit = recordCountLimit;
        this.buffer = Util.newDataBuffer(bufferSizeLimit);
        this.resource = resource;
    }

    @Override
    public void putObject(Object object) throws IOException, InterruptedException {
        int begin = buffer.position();
        serializer.serialize(object, buffer);
        int end = buffer.position();
        addEntry(begin, end);
    }

    private void addEntry(int recordBegin, int recordEnd) throws IOException, InterruptedException {
        int[] sz = sizes;
        int index = recordCount;
        if (index >= sz.length) {
            int[] newSz = new int[Math.max(index + 10, (int) (sz.length * 1.2))];
            System.arraycopy(sz, 0, newSz, 0, sz.length);
            sz = newSz;
            sizes = sz;
        }
        sz[index] = recordEnd - recordBegin;
        recordCount = index + 1;
        if (recordCount >= recordCountLimit || recordEnd >= bufferSizeSoftLimit) {
            flush();
        }
    }

    private void flush() throws IOException, InterruptedException {
        int records = recordCount;
        if (records <= 0) {
            return;
        }
        ByteBuffer buf = buffer.buffer();
        buf.flip();
        try (RecordSink sink = sinks.offer(records, buf.remaining())) {
            int last = 0;
            buf.limit(0);
            int[] sz = sizes;
            for (int i = 0; i < records; i++) {
                int next = last + sz[i];
                buf.position(last).limit(next);
                sink.accept(buf);
                last = next;
            }
        }
        buf.clear();
        recordCount = 0;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try {
            flush();
            buffer.discard();
        } finally {
            if (resource != null) {
                resource.close();
            }
        }
    }
}
