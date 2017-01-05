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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.asakusafw.dag.api.common.DataComparator;
import com.asakusafw.dag.api.common.KeyValueSerializer;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.lang.utils.buffer.nio.NioDataBuffer;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.vanilla.core.util.Buffers;
import com.asakusafw.vanilla.core.util.ExtensibleDataBuffer;

/**
 * An implementation of {@link ObjectWriter} using {@link KeyValueSink}.
 * @since 0.4.0
 */
public class StreamGroupWriter implements ObjectWriter {

    private static final Position[] EMPTY = new Position[0];

    private final KeyValueSink.Stream sinks;

    private final KeyValueSerializer serializer;

    private final DataComparator comparator;

    private final int bufferSizeThreshold;

    private final int recordCountLimit;

    private final ExtensibleDataBuffer buffer;

    private Position[] positions = EMPTY;

    private int recordCount = 0;

    private final InterruptibleIo resource;

    /**
     * Creates a new instance.
     * @param sinks the next sink provider, which accepts sorted key-value pairs
     * @param serializer the object serializer
     * @param comparator the value comparator (nullable)
     * @param bufferSizeLimit the internal buffer size limit in bytes
     * @param recordCountLimit the number of limit records in each page
     */
    public StreamGroupWriter(
            KeyValueSink.Stream sinks,
            KeyValueSerializer serializer, DataComparator comparator,
            int bufferSizeLimit, int recordCountLimit) {
        this(sinks, serializer, comparator,
                bufferSizeLimit, Util.DEFAULT_BUFFER_FLUSH_FACTOR, recordCountLimit,
                null);
    }

    /**
     * Creates a new instance.
     * @param sinks the next sink provider, which accepts sorted key-value pairs
     * @param serializer the object serializer
     * @param comparator the value comparator (nullable)
     * @param bufferSizeLimit the internal buffer size limit in bytes
     * @param bufferFlushFactor the internal buffer flush factor in the limit
     * @param recordCountLimit the number of limit records in each page
     * @param resource the attached resource (nullable)
     */
    public StreamGroupWriter(
            KeyValueSink.Stream sinks,
            KeyValueSerializer serializer, DataComparator comparator,
            int bufferSizeLimit, double bufferFlushFactor, int recordCountLimit,
            InterruptibleIo resource) {
        Arguments.requireNonNull(sinks);
        Arguments.requireNonNull(serializer);
        Arguments.require(bufferSizeLimit > 0);
        Arguments.require(recordCountLimit > 0);
        this.sinks = sinks;
        this.serializer = serializer;
        this.comparator = comparator;
        this.bufferSizeThreshold = Util.getBufferThreshold(bufferSizeLimit, bufferFlushFactor);
        this.recordCountLimit = recordCountLimit;
        this.buffer = Util.newDataBuffer(bufferSizeLimit);
        this.resource = resource;
    }

    @Override
    public void putObject(Object object) throws IOException, InterruptedException {
        int recordBegin = buffer.position();
        serializer.serializeKey(object, buffer);
        int keyEnd = buffer.position();
        serializer.serializeValue(object, buffer);
        int recordEnd = buffer.position();
        addEntry(recordBegin, keyEnd, recordEnd);
    }

    private void addEntry(int recordBegin, int keyEnd, int recordEnd) throws IOException, InterruptedException {
        Position[] ps = positions;
        int index = recordCount;
        if (index >= ps.length) {
            Position[] newPs = new Position[Math.min(
                    recordCountLimit,
                    Math.max(index + 10, (int) (ps.length * 1.2)))];
            System.arraycopy(ps, 0, newPs, 0, ps.length);
            ps = newPs;
            positions = ps;
        }
        Position p = ps[index];
        if (p == null) {
            p = new Position();
            ps[index] = p;
        }
        p.setRange(recordBegin, keyEnd, recordEnd);
        recordCount = index + 1;
        if (recordCount >= recordCountLimit || recordEnd >= bufferSizeThreshold) {
            flush();
        }
    }

    private void flush() throws IOException, InterruptedException {
        if (recordCount <= 0) {
            return;
        }
        buffer.flip();
        sort0();
        flush0();
        buffer.clear();
        recordCount = 0;
    }

    private void sort0() {
        ByteBuffer buf = buffer.buffer();
        ByteBuffer aBuf = Buffers.duplicate(buf);
        ByteBuffer bBuf = Buffers.duplicate(buf);
        NioDataBuffer aWrapper = new NioDataBuffer();
        NioDataBuffer bWrapper = new NioDataBuffer();
        aWrapper.contents = aBuf;
        bWrapper.contents = bBuf;
        DataComparator cmp = comparator;
        Arrays.sort(positions, 0, recordCount, (a, b) -> {
            int keyDiff = a.setKeyRange(aBuf).compareTo(b.setKeyRange(bBuf));
            if (cmp == null || keyDiff != 0) {
                return keyDiff;
            }
            a.setValueRange(aBuf);
            b.setValueRange(bBuf);
            try {
                return cmp.compare(aWrapper, bWrapper);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private void flush0() throws IOException, InterruptedException {
        Position[] ps = positions;
        int records = recordCount;
        int keySize = 0;
        int valueSize = 0;
        for (int i = 0; i < records; i++) {
            Position p = ps[i];
            keySize += p.getKeySize();
            valueSize += p.getValueSize();
        }
        ByteBuffer buf = buffer.buffer();
        ByteBuffer keyBuf = Buffers.duplicate(buf);
        ByteBuffer valueBuf = Buffers.duplicate(buf);
        ByteBuffer lastKeyBuf = Buffers.duplicate(buf);
        try (KeyValueSink sink = sinks.offer(records, keySize, valueSize)) {
            for (int i = 0; i < records; i++) {
                Position p = ps[i];
                if (i != 0 && lastKeyBuf.equals(p.setKeyRange(keyBuf))) {
                    if (sink.accept(p.setValueRange(valueBuf))) {
                        continue;
                    }
                }
                sink.accept(p.setKeyRange(keyBuf), p.setValueRange(valueBuf));
                p.setKeyRange(lastKeyBuf);
            }
        }
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

    private static final class Position {

        private int recordBegin;

        private int keyEnd;

        private int recordEnd;

        Position() {
            return;
        }

        void setRange(int newRecordBegin, int newKeyEnd, int newRecordEnd) {
            this.recordBegin = newRecordBegin;
            this.keyEnd = newKeyEnd;
            this.recordEnd = newRecordEnd;
        }

        int getKeySize() {
            return keyEnd - recordBegin;
        }

        int getValueSize() {
            return recordEnd - keyEnd;
        }

        ByteBuffer setKeyRange(ByteBuffer buffer) {
            return Buffers.range(buffer, recordBegin, keyEnd);
        }

        ByteBuffer setValueRange(ByteBuffer buffer) {
            return Buffers.range(buffer, keyEnd, recordEnd);
        }
    }
}
