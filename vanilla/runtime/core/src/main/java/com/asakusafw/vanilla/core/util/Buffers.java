/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.vanilla.core.engine.BasicVertexScheduler;

/**
 * Utilities about {@link ByteBuffer}.
 * @since 0.4.0
 */
public final class Buffers {

    private static final Logger LOG = LoggerFactory.getLogger(BasicVertexScheduler.class);

    private static final String KEY_PREFIX = SystemProperty.KEY_PREFIX + "buffer."; //$NON-NLS-1$

    /**
     * The system property key of whether or not the heap buffer is instead of direct buffer
     * ({@value}: {@value #DEFAULT_HEAP}).
     */
    public static final String KEY_HEAP = KEY_PREFIX + "heap"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_HEAP}.
     */
    public static final boolean DEFAULT_HEAP = false;

    /**
     * The system property key of the buffer expansion factor ({@value}: {@value #DEFAULT_EXPANSION_FACTOR}).
     */
    public static final String KEY_EXPANSION_FACTOR = KEY_PREFIX + "expansion.factor"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_EXPANSION_FACTOR}.
     */
    public static final double DEFAULT_EXPANSION_FACTOR = 2.0;

    /**
     * The system property key of the maximum buffer utility ratio of shrink targets
     * ({@value}: {@value #DEFAULT_MAX_SHRINK_UTILITY}).
     * If the target buffer utility ratio is greater than this, the buffer will be never shrunk.
     */
    public static final String KEY_MAX_SHRINK_UTILITY = KEY_PREFIX + "shrink.utility.max"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_MAX_SHRINK_UTILITY}.
     */
    public static final double DEFAULT_MAX_SHRINK_UTILITY = 0.5;

    /**
     * The system property key of the minimum buffer capacity of shrink targets
     * ({@value}: {@value #DEFAULT_MAX_SHRINK_UTILITY}).
     * If the target buffer capacity is less than this, the buffer will be never shrunk.
     */
    public static final String KEY_MIN_SHRINK_CAPACITY = KEY_PREFIX + "shrink.capacity.min"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_MIN_SHRINK_CAPACITY}.
     */
    public static final int DEFAULT_MIN_SHRINK_CAPACITY = 64 * 1024;

    static final int MIN_SIZE = 64 * 1024;

    static final boolean HEAP = SystemProperty.get(KEY_HEAP, DEFAULT_HEAP);

    static final double EXPANSION_FACTOR = SystemProperty.get(KEY_EXPANSION_FACTOR, DEFAULT_EXPANSION_FACTOR);

    static final double MAX_SHRINK_UTILITY = SystemProperty.get(KEY_MAX_SHRINK_UTILITY, DEFAULT_MAX_SHRINK_UTILITY);

    static final int MIN_SHRINK_CAPACITY = SystemProperty.get(KEY_MIN_SHRINK_CAPACITY, DEFAULT_MIN_SHRINK_CAPACITY);

    static {
        if (LOG.isDebugEnabled()) {
            LOG.debug("buffers:");
            LOG.debug("  {}: {}", KEY_HEAP, HEAP);
            LOG.debug("  {}: {}", KEY_EXPANSION_FACTOR, EXPANSION_FACTOR);
            LOG.debug("  {}: {}", KEY_MAX_SHRINK_UTILITY, MAX_SHRINK_UTILITY);
            LOG.debug("  {}: {}", KEY_MIN_SHRINK_CAPACITY, MIN_SHRINK_CAPACITY);
        }
    }

    private Buffers() {
        return;
    }

    /**
     * Allocates a new {@link ByteBuffer}.
     * @param size the buffer size in bytes
     * @return the allocated buffer
     */
    public static ByteBuffer allocate(int size) {
        if (HEAP) {
            return ByteBuffer.allocate(size).order(ByteOrder.nativeOrder());
        } else {
            return ByteBuffer.allocateDirect(size).order(ByteOrder.nativeOrder());
        }
    }

    /**
     * Returns a slice of the buffer with sharing contents.
     * @param buffer the target buffer
     * @return a slice
     */
    public static ByteBuffer slice(ByteBuffer buffer) {
        return buffer.slice().order(buffer.order());
    }

    /**
     * Returns a copy of the buffer with sharing contents.
     * @param buffer the target buffer
     * @return a copy
     */
    public static ByteBuffer duplicate(ByteBuffer buffer) {
        return buffer.duplicate().order(buffer.order());
    }

    /**
     * Sets the range of the given buffer.
     * @param buffer the target buffer
     * @param position the buffer position
     * @param limit the buffer limit
     * @return the given buffer
     */
    public static ByteBuffer range(ByteBuffer buffer, int position, int limit) {
        buffer.clear().position(position).limit(limit);
        return buffer;
    }

    /**
     * Shrinks the given buffer only if it has enough unused space.
     * @param buffer the target buffer
     * @return the shrunk buffer
     */
    public static ByteBuffer shrink(ByteBuffer buffer) {
        ByteBuffer copy = Buffers.duplicate(buffer);
        if (copy.capacity() < MIN_SHRINK_CAPACITY
                || copy.remaining() > copy.capacity() * MAX_SHRINK_UTILITY) {
            return copy;
        }
        ByteBuffer compact = Buffers.allocate(copy.remaining());
        compact.put(copy);
        compact.flip();
        return compact;
    }
}
