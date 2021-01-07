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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import com.asakusafw.vanilla.core.util.Buffers;

/**
 * A {@link ReadableByteChannel} for written by {@link Lz4FramedWritableByteChannel}.
 * @since 0.5.3
 */
public class Lz4FramedReadableByteChannel implements ReadableByteChannel {

    private final ReadableByteChannel channel;

    private volatile ByteBuffer uncompressedFrameBuffer;

    private ByteBuffer compressedFrameBuffer;

    /**
     * creates a new instance.
     * @param channel the destination channel
     */
    public Lz4FramedReadableByteChannel(ReadableByteChannel channel) {
        this.channel = channel;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        ByteBuffer src = fill();
        if (src == null) {
            return -1;
        }
        return Buffers.put(src, dst);
    }

    private ByteBuffer fill() throws IOException {
        ByteBuffer uncompressed = uncompressedFrameBuffer;
        if (uncompressed == null) {
            int uncompressedFrameSize = readFramedChannelHeader(channel);
            uncompressed = Buffers.allocate(uncompressedFrameSize);
            uncompressed.flip();

            this.uncompressedFrameBuffer = uncompressed;
            this.compressedFrameBuffer = Buffers.allocate(getMaxCompressedFrameSize(uncompressedFrameSize));
        }

        if (!uncompressed.hasRemaining()) {
            // first, obtain compressed frame info
            ByteBuffer compressed = compressedFrameBuffer;
            compressed.clear();
            compressed.limit(Integer.BYTES);
            do {
                int read = channel.read(compressed);
                if (read == -1) {
                    if (compressed.position() == 0) {
                        // return null if no more compressed frames
                        return null;
                    } else {
                        throw new EOFException();
                    }
                }
            } while (compressed.hasRemaining());
            compressed.flip();
            int compressedFrameInfo = compressed.getInt();
            int compressedDataSize = compressedFrameInfo & COMPRESSED_DATA_SIZE_MASK;
            boolean isCompleteFrame = (compressedFrameInfo & INCOMPLETE_FRAME_FLAG) == 0;

            // next, read the rest of compressed frame
            compressed.clear();
            compressed.limit(compressedDataSize + (isCompleteFrame ? 0 : Integer.BYTES));
            do {
                int read = channel.read(compressed);
                if (read < 0) {
                    throw new EOFException();
                }
            } while (compressed.hasRemaining());
            compressed.flip();

            // finally, uncompress the frame data
            uncompressed.clear();
            if (!isCompleteFrame) {
                // limit destination buffer for incomplete frames
                int uncompresssedFrameSize = compressed.getInt();
                uncompressed.limit(uncompresssedFrameSize);
            }
            FRAME_DECOMPRESSOR.decompress(compressed, uncompressed);
            uncompressed.flip();
        }

        return uncompressed;
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
