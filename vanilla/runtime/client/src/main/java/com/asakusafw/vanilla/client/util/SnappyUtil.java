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
package com.asakusafw.vanilla.client.util;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import com.asakusafw.vanilla.core.util.Buffers;
import com.asakusafw.vanilla.core.util.SystemProperty;

/**
 * Utilities about snappy compression.
 */
public final class SnappyUtil {

    private static final Logger LOG = LoggerFactory.getLogger(SnappyUtil.class);

    private static final String KEY_PREFIX = SystemProperty.KEY_PREFIX + "snappy."; //$NON-NLS-1$

    /**
     * The system property key of snappy compression frame size for framed compressor
     * ({@value}: {@value #DEFAULT_FRAME_SIZE}).
     * @see SnappyFramedWritableByteChannel
     */
    public static final String KEY_FRAME_SIZE = KEY_PREFIX + "frame.size"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_FRAME_SIZE}.
     */
    public static final int DEFAULT_FRAME_SIZE = 16 * 1024;

    /**
     * The minimum value of {@link #KEY_FRAME_SIZE}.
     */
    public static final int MIN_FRAME_SIZE = 4 * 1024;

    /**
     * The maximum value of {@link #KEY_FRAME_SIZE}.
     */
    public static final int MAX_FRAME_SIZE = 1024 * 1024;

    static final int FRAME_SIZE = Math.max(
            MIN_FRAME_SIZE,
            Math.min(
                    MAX_FRAME_SIZE,
                    SystemProperty.get(KEY_FRAME_SIZE, DEFAULT_FRAME_SIZE)));

    /*
     * struct framed_file {
     *   int8_t magic[4];
     *   int32_t frame_size;
     *
     *   struct compressed_frame_t {
     *     int32_t compressed_frame_info;
     *     int8_t compressed_data[compressed_frame_info & 0xff_ffff];
     *   } compressed_frames[...];
     * }
     */

    private static final byte[] FRAMED_FILE_HEADER_MAGIC = {
            (byte) 'S',
            (byte) 'N',
            (byte) 'P',
            (byte) 'f',
    };

    static final int FRAMED_FILE_HEADER_SIZE = FRAMED_FILE_HEADER_MAGIC.length + Integer.BYTES;

    private static final ThreadLocal<ByteBuffer> BUFFERS = ThreadLocal
            .withInitial(() -> Buffers.allocate(FRAMED_FILE_HEADER_SIZE));

    static final int FRAME_HEADER_SIZE = Integer.BYTES;

    static final int COMPRESSED_DATA_SIZE_MASK = 0x00ff_ffff;

    static int getMaxCompressedFrameSize(int rawFrameSize) {
        int maxCompressedDataSize = Snappy.maxCompressedLength(rawFrameSize);
        assert maxCompressedDataSize <= COMPRESSED_DATA_SIZE_MASK;
        return FRAME_HEADER_SIZE + maxCompressedDataSize;
    }

    static void writeFramedChannelHeader(WritableByteChannel channel, int frameSize) throws IOException {
        ByteBuffer buf = BUFFERS.get();
        buf.clear();
        buf.put(FRAMED_FILE_HEADER_MAGIC);
        buf.putInt(frameSize);
        buf.flip();
        do {
            channel.write(buf);
        } while (buf.hasRemaining());
    }

    static int readFramedChannelHeader(ReadableByteChannel channel) throws IOException {
        ByteBuffer buf = BUFFERS.get();
        buf.clear();
        do {
            int read = channel.read(buf);
            if (read == -1) {
                throw new EOFException();
            }
        } while (buf.hasRemaining());
        buf.flip();
        for (byte h : FRAMED_FILE_HEADER_MAGIC) {
            byte b = buf.get();
            if (b != h) {
                throw new IOException("invalid snappy frame header");
            }
        }
        return buf.getInt();
    }

    static {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Snappy:");
            LOG.debug("  {}: {}", KEY_FRAME_SIZE, FRAME_SIZE);
        }
    }

    private SnappyUtil() {
        return;
    }
}
