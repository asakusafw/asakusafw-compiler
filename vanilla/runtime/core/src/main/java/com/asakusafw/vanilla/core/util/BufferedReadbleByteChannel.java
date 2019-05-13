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
package com.asakusafw.vanilla.core.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * A {@link ReadableByteChannel} with I/O buffer.
 * @since 0.5.3
 */
public class BufferedReadbleByteChannel implements ReadableByteChannel {

    private final ReadableByteChannel channel;

    private final ByteBuffer buffer;

    /**
     * Creates a new instance.
     * @param channel the source channel
     * @see Buffers#KEY_INPUT_CHANNEL_BUFFER_SIZE
     */
    public BufferedReadbleByteChannel(ReadableByteChannel channel) {
        this(channel, Buffers.INPUT_CHANNEL_BUFFER_SIZE);
    }

    /**
     * Creates a new instance.
     * @param channel the source channel
     * @param bufferSize the buffer size in bytes
     */
    public BufferedReadbleByteChannel(ReadableByteChannel channel, int bufferSize) {
        this.channel = channel;
        this.buffer = Buffers.allocate(bufferSize);
        buffer.flip();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        while (true) {
            // copies buffered contents into the destination buffer if remained
            if (buffer.hasRemaining()) {
                return Buffers.put(buffer, dst);
            }

            if (dst.remaining() >= buffer.capacity()) {
                return channel.read(dst);
            }

            buffer.clear();
            int read = channel.read(buffer);
            buffer.flip();
            if (read <= 0) {
                return read;
            }
        }
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
