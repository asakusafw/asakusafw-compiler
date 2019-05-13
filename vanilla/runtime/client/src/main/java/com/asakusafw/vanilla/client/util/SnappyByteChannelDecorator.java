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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import com.asakusafw.vanilla.core.io.ByteChannelDecorator;
import com.asakusafw.vanilla.core.util.SystemProperty;

/**
 * An implementation of {@link ByteChannelDecorator} which using snappy compression.
 * @since 0.5.3
 */
public class SnappyByteChannelDecorator implements ByteChannelDecorator {

    private static final Logger LOG = LoggerFactory.getLogger(SnappyByteChannelDecorator.class);

    private static final String KEY_PREFIX = SystemProperty.KEY_PREFIX + "snappy."; //$NON-NLS-1$

    /**
     * The system property key of snappy compression frame size for framed compressor
     * ({@value}: {@value #DEFAULT_BLOCK_SIZE}).
     */
    public static final String KEY_BLOCK_SIZE = KEY_PREFIX + "frame.size"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_BLOCK_SIZE}.
     */
    public static final int DEFAULT_BLOCK_SIZE = SnappyFramedOutputStream.DEFAULT_BLOCK_SIZE;

    /**
     * The minimum value of {@link #KEY_BLOCK_SIZE}.
     */
    public static final int MIN_BLOCK_SIZE = 4 * 1024;

    /**
     * The maximum value of {@link #KEY_BLOCK_SIZE}.
     */
    public static final int MAX_BLOCK_SIZE = SnappyFramedOutputStream.MAX_BLOCK_SIZE;

    /**
     * The system property key of required minimum compression ratio for each frame.
     * If a frame was exceeded this ratio, we use its uncompressed data instead of compressed one.
     * ({@value}: {@value #DEFAULT_COMPRESSION_THRESHOLD}).
     */
    public static final String KEY_COMPRESSION_THRESHOLD = KEY_PREFIX + "frame.compressionThreshold"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_COMPRESSION_THRESHOLD}.
     */
    public static final double DEFAULT_COMPRESSION_THRESHOLD =
            SnappyFramedOutputStream.DEFAULT_MIN_COMPRESSION_RATIO;

    /**
     * The minimum value of {@link #KEY_COMPRESSION_THRESHOLD}.
     */
    public static final double MIN_COMPRESSION_THRESHOLD = 0.0;

    /**
     * The maximum value of {@link #KEY_COMPRESSION_THRESHOLD}.
     */
    public static final double MAX_COMPRESSION_THRESHOLD = 1.0;

    /**
     * The system property key of whether or not verifying compressed data while extraction.
     * ({@value}: {@value #DEFAULT_VERIFY_CHECKSUM}).
     */
    public static final String KEY_VERIFY_CHECKSUM = KEY_PREFIX + "frame.verifyChecksum"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_VERIFY_CHECKSUM}.
     */
    public static final boolean DEFAULT_VERIFY_CHECKSUM = false;

    static final int BLOCK_SIZE = Math.max(
            MIN_BLOCK_SIZE,
            Math.min(
                    MAX_BLOCK_SIZE,
                    SystemProperty.get(KEY_BLOCK_SIZE, DEFAULT_BLOCK_SIZE)));

    static final double COMPRESSION_THRESHOLD = Math.max(
            MIN_COMPRESSION_THRESHOLD,
            Math.min(
                    MAX_COMPRESSION_THRESHOLD,
                    SystemProperty.get(KEY_COMPRESSION_THRESHOLD, DEFAULT_COMPRESSION_THRESHOLD)));

    static final boolean VERIFY_CHECKSUM =
            SystemProperty.get(KEY_VERIFY_CHECKSUM, DEFAULT_VERIFY_CHECKSUM);

    static {
        if (LOG.isDebugEnabled()) {
            LOG.debug("snappy:");
            LOG.debug("  {}: {}", KEY_BLOCK_SIZE, BLOCK_SIZE);
            LOG.debug("  {}: {}", KEY_COMPRESSION_THRESHOLD, COMPRESSION_THRESHOLD);
            LOG.debug("  {}: {}", KEY_VERIFY_CHECKSUM, VERIFY_CHECKSUM);
        }
    }

    @Override
    public ReadableByteChannel decorate(ReadableByteChannel channel) throws IOException {
        return new SnappyFramedInputStream(channel, VERIFY_CHECKSUM);
    }

    @Override
    public WritableByteChannel decorate(WritableByteChannel channel) throws IOException {
        return new SnappyFramedOutputStream(channel, BLOCK_SIZE, COMPRESSION_THRESHOLD);
    }
}
