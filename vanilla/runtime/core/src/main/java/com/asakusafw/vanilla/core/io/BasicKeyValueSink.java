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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * A basic implementation of {@link KeyValueSink}.
 * <h3>buffer layout</h3>
<pre><code>
struct item {
    s4 size;
    u1 contents[size];
};
struct pair {
    struct item key;
    struct item values[];
    u4 EOR = -1;
};
struct record_buffer {
    struct pair records[];
    u4 EOF = -1;
};
</code></pre>
 * @since 0.4.0
 * @version 0.5.3
 */
public final class BasicKeyValueSink implements KeyValueSink {

    private final DataWriter.Channel channel;

    private DataWriter writer;

    private boolean sawKey = false;

    private boolean written = false;

    /**
     * Creates a new instance.
     * @param channel the destination channel
     * @param size the total data size in bytes
     * @throws IOException if I/O error was occurred while initializing this object
     * @throws InterruptedException if interrupted while initializing this object
     */
    public BasicKeyValueSink(DataWriter.Channel channel, int size) throws IOException, InterruptedException {
        Arguments.requireNonNull(channel);
        this.channel = channel;
        this.writer = channel.acquire(size);
    }

    private BasicKeyValueSink(DataWriter writer) {
        Arguments.requireNonNull(writer);
        this.channel = null;
        this.writer = writer;
    }

    /**
     * Creates a new stream of {@link BasicKeyValueSink}.
     * @param channel the destination channel
     * @return the created stream
     */
    public static Stream stream(DataWriter.Channel channel) {
        Arguments.requireNonNull(channel);
        return (recordCount, keySize, valueSize) -> {
            long total = 0;
            total += (long) recordCount * Integer.BYTES * 3; // record_buffer.records[].{{key, values[]}.size, EOR}
            total += keySize;
            total += valueSize;
            total += Integer.BYTES; // EOF
            Arguments.require(total <= Integer.MAX_VALUE);
            return new BasicKeyValueSink(channel, (int) total);
        };
    }

    @Override
    public void accept(ByteBuffer key, ByteBuffer value) throws IOException, InterruptedException {
        DataWriter w = writer;
        Invariants.requireNonNull(w);
        keyBreak(w);
        addItem(w, key);
        addItem(w, value);
        sawKey = true;
        written = true;
    }

    @Override
    public boolean accept(ByteBuffer value) throws IOException, InterruptedException {
        if (sawKey) {
            DataWriter w = writer;
            Invariants.requireNonNull(w);
            addItem(w, value);
            return true;
        } else {
            return false;
        }
    }

    private void keyBreak(DataWriter w) throws IOException, InterruptedException {
        if (sawKey) {
            addDelim(w);
            sawKey = false;
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try (DataWriter w = release()) {
            if (w == null) {
                return;
            }
            // commit only if any entries exist
            if (written && channel != null) {
                channel.commit(w);
            }
        }
    }

    /**
     * Releases the target {@link DataWriter}.
     * @return the target data writer, or {@code null} if it does not exist
     * @throws IOException if I/O error was occurred while finalizing the sink
     * @throws InterruptedException if interrupted while finalizing the sink
     * @since 0.5.3
     */
    public DataWriter release() throws IOException, InterruptedException {
        DataWriter w = writer;
        if (w == null) {
            return null;
        }

        // add EOF mark of the last group, only if it exists
        keyBreak(w);

        // add EOF mark of file
        addDelim(w);

        writer = null;
        return w;
    }

    private static void addItem(DataWriter writer, ByteBuffer buffer) throws IOException, InterruptedException {
        writer.writeInt(buffer.remaining());
        writer.writeFully(buffer);
    }

    private static void addDelim(DataWriter writer) throws IOException, InterruptedException {
        writer.writeInt(-1);
    }

    static final int BUFFER_PADDING = 64;

    /**
     * Copies key value pairs into the destination writer.
     * @param source the source cursor
     * @param destination the destination writer
     * @return the written size in bytes
     * @throws IOException if I/O error was occurred while copying contents
     * @throws InterruptedException if interrupted while copying contents
     */
    public static long copy(KeyValueCursor source, DataWriter destination) throws IOException, InterruptedException {
        @SuppressWarnings("resource")
        BasicKeyValueSink sink = new BasicKeyValueSink(destination);
        long size = 0;
        ByteBuffer lastKey = null;
        while (source.next()) {
            ByteBuffer key = source.getKey();
            ByteBuffer value = source.getValue();
            if (!key.equals(lastKey)) {
                // next group
                if (lastKey == null || lastKey.capacity() < key.remaining()) {
                    lastKey = Buffers.allocate(key.remaining() + BUFFER_PADDING);
                }
                int position = key.position();
                lastKey.clear();
                lastKey.put(key);
                lastKey.flip();
                key.position(position);

                // note: temporary count EOF of the last group, even if this is first group
                size += Integer.BYTES; // the last group EOF
                size += Integer.BYTES + key.remaining(); // key item
                size += Integer.BYTES + value.remaining(); // value item

                sink.accept(key, value);
            } else {
                size += Integer.BYTES + value.remaining(); // value item

                // continue group
                sink.accept(value);
            }
        }

        // note: don't count EOF of the last group, because we've already considered extra EOF in first group
        sink.release();
        size += Integer.BYTES; // file EOF
        return size;
    }
}
