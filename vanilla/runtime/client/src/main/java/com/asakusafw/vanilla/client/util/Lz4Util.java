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

import com.asakusafw.vanilla.core.util.Buffers;
import com.asakusafw.vanilla.core.util.SystemProperty;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Utilities about LZ4 compression.
 */
public final class Lz4Util {

    private static final Logger LOG = LoggerFactory.getLogger(Lz4Util.class);

    private static final String KEY_PREFIX = SystemProperty.KEY_PREFIX + "lz4."; //$NON-NLS-1$

    /**
     * The system property key of whether or not native LZ4 library is enabled if it is available
     * ({@value}: {@value #DEFAULT_NATIVE}).
     */
    public static final String KEY_NATIVE = KEY_PREFIX + "native"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_NATIVE}.
     */
    public static final boolean DEFAULT_NATIVE = true;

    /**
     * The system property key of LZ4 compression frame size for framed compressor
     * ({@value}: {@value #DEFAULT_FRAME_SIZE}).
     * @see Lz4FramedWritableByteChannel
     */
    public static final String KEY_FRAME_SIZE = KEY_PREFIX + "frame.size"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_FRAME_SIZE}.
     */
    public static final int DEFAULT_FRAME_SIZE = 64 * 1024;

    /**
     * The minimum value of {@link #KEY_FRAME_SIZE}.
     */
    public static final int MIN_FRAME_SIZE = 4 * 1024;

    /**
     * The maximum value of {@link #KEY_FRAME_SIZE}.
     */
    public static final int MAX_FRAME_SIZE = 1024 * 1024;

    /**
     * The system property key of LZ4 compression frame size for framed compressor
     * ({@value}: {@value #DEFAULT_FRAME_COMPRESSION_LEVEL} - fast compression).
     * @see Lz4FramedWritableByteChannel
     */
    public static final String KEY_FRAME_COMPRESSION_LEVEL = KEY_PREFIX + "frame.compressionLevel"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_FRAME_COMPRESSION_LEVEL}.
     */
    public static final int DEFAULT_FRAME_COMPRESSION_LEVEL = 0;

    /**
     * The minimum value of {@link #KEY_FRAME_COMPRESSION_LEVEL}.
     */
    public static final int MIN_FRAME_COMPRESSION_LEVEL = 0;

    /**
     * The maximum value of {@link #KEY_FRAME_COMPRESSION_LEVEL}.
     */
    public static final int MAX_FRAME_COMPRESSION_LEVEL = 17;

    static final boolean NATIVE = SystemProperty.get(KEY_NATIVE, DEFAULT_NATIVE);

    static final int FRAME_SIZE = Math.max(
            MIN_FRAME_SIZE,
            Math.min(
                    MAX_FRAME_SIZE,
                    SystemProperty.get(KEY_FRAME_SIZE, DEFAULT_FRAME_SIZE)));

    static final int FRAME_COMPRESSION_LEVEL = Math.max(
            MIN_FRAME_COMPRESSION_LEVEL,
            Math.min(
                    MAX_FRAME_COMPRESSION_LEVEL,
                    SystemProperty.get(KEY_FRAME_COMPRESSION_LEVEL, DEFAULT_FRAME_COMPRESSION_LEVEL)));

    static final LZ4Factory FACTORY = NATIVE ? LZ4Factory.fastestInstance() : LZ4Factory.fastestJavaInstance();

    static final LZ4Compressor FRAME_COMPRESSOR = getCompressor(FRAME_COMPRESSION_LEVEL);

    static final LZ4FastDecompressor FRAME_DECOMPRESSOR = FACTORY.fastDecompressor();

    static LZ4Compressor getCompressor(int compressionLevel) {
       if (compressionLevel == 0) {
           return FACTORY.fastCompressor();
       }
       return FACTORY.highCompressor(compressionLevel);
    }

    /*
     * struct framed_file {
     *   int8_t magic[4];
     *   int32_t frame_size;
     *
     *   struct compressed_frame_t {
     *     int32_t compressed_frame_info;
     *     int32_t uncompressed_frame_size[(compressed_frame_info >> 24) & 0x01];
     *     int8_t compressed_data[compressed_frame_info & 0xff_ffff];
     *   } compressed_frames[...];
     * }
     */

    private static final byte[] FRAMED_FILE_HEADER_MAGIC = {
            (byte) 'L',
            (byte) 'Z',
            (byte) '4',
            (byte) 'f',
    };

    static final int FRAMED_FILE_HEADER_SIZE = FRAMED_FILE_HEADER_MAGIC.length + Integer.BYTES;

    private static final ThreadLocal<ByteBuffer> BUFFERS = ThreadLocal
            .withInitial(() -> Buffers.allocate(FRAMED_FILE_HEADER_SIZE));

    static final int COMPLETE_FRAME_HEADER_SIZE = Integer.BYTES * 1;

    static final int INCOMPLETE_FRAME_HEADER_SIZE = Integer.BYTES * 2;

    static final int INCOMPLETE_FRAME_FLAG = 0x0100_0000;

    static final int COMPRESSED_DATA_SIZE_MASK = 0x00ff_ffff;

    static int getMaxCompressedFrameSize(int rawFrameSize) {
        int maxCompressedDataSize = FRAME_COMPRESSOR.maxCompressedLength(rawFrameSize);
        assert maxCompressedDataSize <= COMPRESSED_DATA_SIZE_MASK;
        return INCOMPLETE_FRAME_HEADER_SIZE + maxCompressedDataSize;
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
                throw new IOException("invalid LZ4 frame header");
            }
        }
        return buf.getInt();
    }

    static {
        if (LOG.isDebugEnabled()) {
            LOG.debug("LZ4:");
            LOG.debug("  {}: {}", KEY_NATIVE, NATIVE);
            LOG.debug("  {}: {}", KEY_FRAME_SIZE, FRAME_SIZE);
            LOG.debug("  {}: {}", KEY_FRAME_COMPRESSION_LEVEL, FRAME_COMPRESSION_LEVEL);
            LOG.debug("  active implementation: {}", FACTORY);
        }
    }

    private Lz4Util() {
        return;
    }
}
