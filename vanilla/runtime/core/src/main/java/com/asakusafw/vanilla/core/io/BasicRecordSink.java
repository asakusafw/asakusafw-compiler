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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * A basic implementation of {@link RecordSink}.
 *
 * <h3>buffer layout</h3>
<pre><code>
struct item {
    s4 size;
    u1 contents[size];
};
struct record_buffer {
    struct item records[];
    u4 EOF = -1;
};
</code></pre>
 * @since 0.4.0
 */
public final class BasicRecordSink implements RecordSink {

    private final DataWriter.Channel channel;

    private DataWriter writer;

    private boolean written = false;

    /**
     * Creates a new instance.
     * @param channel the destination channel
     * @param size the total data size in bytes
     * @throws IOException if I/O error was occurred while initializing this object
     * @throws InterruptedException if interrupted while initializing this object
     */
    public BasicRecordSink(DataWriter.Channel channel, int size) throws IOException, InterruptedException {
        Arguments.requireNonNull(channel);
        this.channel = channel;
        this.writer = channel.acquire(size);
    }

    /**
     * Creates a new stream of {@link BasicRecordSink}.
     * @param channel the destination channel
     * @return the created stream
     */
    public static Stream stream(DataWriter.Channel channel) {
        Arguments.requireNonNull(channel);
        return (recordCount, contentSize) -> {
            long total = 0;
            total += (long) recordCount * Integer.BYTES; // size fields
            total += contentSize;
            total += Integer.BYTES; // EOF
            Arguments.require(total <= Integer.MAX_VALUE);
            return new BasicRecordSink(channel, (int) total);
        };
    }

    @Override
    public void accept(ByteBuffer contents) throws IOException, InterruptedException {
        DataWriter w = writer;
        Invariants.requireNonNull(w);
        w.writeInt(contents.remaining());
        w.writeFully(contents);
        written = true;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try (DataWriter w = writer) {
            if (w == null) {
                return;
            }
            writer = null;
            if (written) {
                w.writeInt(-1);
                channel.commit(w);
            }
        }
    }
}
