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

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A {@link WritableByteChannel} with I/O buffer.
 * @since 0.5.3
 */
public class BufferedWritableByteChannel implements WritableByteChannel, Flushable {

    private final WritableByteChannel channel;

    private final ByteBuffer buffer;

    /**
     * Creates a new instance.
     * @param channel the destination channel
     * @see Buffers#KEY_OUTPUT_CHANNEL_BUFFER_SIZE
     */
    public BufferedWritableByteChannel(WritableByteChannel channel) {
        this(channel, Buffers.OUTPUT_CHANNEL_BUFFER_SIZE);
    }

    /**
     * Creates a new instance.
     * @param channel the destination channel
     * @param bufferSize the buffer size in bytes
     */
    public BufferedWritableByteChannel(WritableByteChannel channel, int bufferSize) {
        this.channel = channel;
        this.buffer = Buffers.allocate(bufferSize);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (buffer.hasRemaining()) {
            return Buffers.put(src, buffer);
        }

        flush();
        if (src.remaining() >= buffer.remaining()) {
            return channel.write(src);
        }

        int size = src.remaining();
        buffer.put(src);
        return size;
    }

    @Override
    public void flush() throws IOException {
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        buffer.clear();
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        flush();
        channel.close();
    }
}
