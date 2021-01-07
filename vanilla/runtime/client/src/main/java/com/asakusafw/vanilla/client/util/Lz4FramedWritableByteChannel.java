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

import static com.asakusafw.vanilla.client.util.Lz4Util.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.asakusafw.vanilla.core.util.Buffers;

/**
 * A {@link WritableByteChannel} with LZ4 compression by each frame.
 * @since 0.5.3
 */
public class Lz4FramedWritableByteChannel implements WritableByteChannel {

    private final WritableByteChannel channel;

    private final ByteBuffer uncompressedFrameBuffer;

    private volatile ByteBuffer compressedFrameBuffer;

    /**
     * creates a new instance.
     * @param channel the destination channel
     */
    public Lz4FramedWritableByteChannel(WritableByteChannel channel) {
        this(channel, FRAME_SIZE);
    }

    /**
     * creates a new instance.
     * @param channel the destination channel
     * @param frameSize the compression frame size in bytes
     */
    public Lz4FramedWritableByteChannel(WritableByteChannel channel, int frameSize) {
        this.channel = channel;
        this.uncompressedFrameBuffer = Buffers.allocate(Math.max(MIN_FRAME_SIZE, Math.min(MAX_FRAME_SIZE, frameSize)));
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
            dst = Buffers.allocate(getMaxCompressedFrameSize(uncompressedFrameSize));
            this.compressedFrameBuffer = dst;
        }
        src.flip();
        if (src.hasRemaining()) {
            int uncompressedFrameSize = src.remaining();
            boolean isCompleteFrame = src.capacity() == uncompressedFrameSize;
            int headerSize = isCompleteFrame ? COMPLETE_FRAME_HEADER_SIZE : INCOMPLETE_FRAME_HEADER_SIZE;

            dst.clear();
            dst.position(headerSize);
            FRAME_COMPRESSOR.compress(src, dst);
            dst.flip();
            int compressedDataSize = dst.limit() - headerSize;

            if (isCompleteFrame) {
                dst.putInt(0, compressedDataSize);
            } else {
                dst.putInt(0, compressedDataSize | INCOMPLETE_FRAME_FLAG);
                dst.putInt(Integer.BYTES, uncompressedFrameSize);
            }

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
