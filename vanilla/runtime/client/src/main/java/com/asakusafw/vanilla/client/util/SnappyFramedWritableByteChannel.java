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
package com.asakusafw.vanilla.client.util;

import static com.asakusafw.vanilla.client.util.SnappyUtil.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.xerial.snappy.Snappy;

import com.asakusafw.vanilla.core.util.Buffers;

/**
 * A {@link WritableByteChannel} with snappy compression by each frame.
 * @since 0.5.3
 */
public class SnappyFramedWritableByteChannel implements WritableByteChannel {

    private final WritableByteChannel channel;

    private final ByteBuffer uncompressedFrameBuffer;

    private volatile ByteBuffer compressedFrameBuffer;

    /**
     * creates a new instance.
     * @param channel the destination channel
     */
    public SnappyFramedWritableByteChannel(WritableByteChannel channel) {
        this(channel, FRAME_SIZE);
    }

    /**
     * creates a new instance.
     * @param channel the destination channel
     * @param frameSize the compression frame size in bytes
     */
    public SnappyFramedWritableByteChannel(WritableByteChannel channel, int frameSize) {
        this.channel = channel;
        int size = Math.max(MIN_FRAME_SIZE, Math.min(MAX_FRAME_SIZE, frameSize));
        this.uncompressedFrameBuffer = SnappyUtil.allocateBuffer(size);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        ByteBuffer dst = uncompressedFrameBuffer;
        int written = 0;
        while (src.hasRemaining()) {
            written += Buffers.put(src, dst);
            if (!dst.hasRemaining()) {
                flush();
            }
        }
        return written;
    }

    private void flush() throws IOException {
        ByteBuffer src = uncompressedFrameBuffer;
        ByteBuffer dst = compressedFrameBuffer;
        if (dst == null) {
            int uncompressedFrameSize = uncompressedFrameBuffer.capacity();
            writeFramedChannelHeader(channel, uncompressedFrameSize);
            dst = SnappyUtil.allocateBuffer(getMaxCompressedFrameSize(uncompressedFrameSize));
            this.compressedFrameBuffer = dst;
        }
        src.flip();
        if (src.hasRemaining()) {
            dst.clear();
            dst.position(FRAME_HEADER_SIZE);
            int compressedDataSize = Snappy.compress(src, dst);

            // Snappy.compress() does not change the destination buffer position
            Buffers.range(dst, 0, FRAME_HEADER_SIZE + compressedDataSize);
            dst.putInt(0, compressedDataSize);

            do {
                channel.write(dst);
            } while (dst.hasRemaining());
        }
        src.clear();
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
