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
package com.asakusafw.vanilla.core.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.utils.buffer.DataBuffer;
import com.asakusafw.lang.utils.buffer.DataIoUtils;

/**
 * A extensible {@link DataBuffer}.
 */
public final class ExtensibleDataBuffer implements DataBuffer {

    static final Logger LOG = LoggerFactory.getLogger(ExtensibleDataBuffer.class);

    private static final ByteBuffer EMPTY_BUFFER = Buffers.allocate(0);

    private static final int BUFFER_MARGIN = 4 * 1024;

    static final double LIMITED_EXPANSION_FACTOR = 1.1;

    private final int initial;

    private final int softLimit;

    private final double expansionFactor;

    private ByteBuffer contents = EMPTY_BUFFER;

    /**
     * Creates a new instance.
     * @param initial the initial buffer capacity in bytes
     * @param limit the soft-limit of capacity in bytes
     */
    public ExtensibleDataBuffer(int initial, int limit) {
        this(initial, limit, Buffers.EXPANSION_FACTOR);
    }

    /**
     * Creates a new instance.
     * @param initial the initial buffer capacity in bytes
     * @param limit the soft-limit of capacity in bytes
     * @param expansionFactor the buffer expansion factor
     */
    public ExtensibleDataBuffer(int initial, int limit, double expansionFactor) {
        this.initial = initial;
        this.softLimit = limit;
        this.expansionFactor = Math.max(expansionFactor, LIMITED_EXPANSION_FACTOR);
        if (LOG.isTraceEnabled()) {
            LOG.trace("data buffer: {}(*{})/{}", initial, expansionFactor, limit);
        }
    }

    /**
     * Returns the internal buffer.
     * @return the internal buffer
     */
    public ByteBuffer buffer() {
        return contents;
    }

    /**
     * Returns the current buffer position.
     * @return the buffer position
     */
    public int position() {
        return contents.position();
    }

    /**
     * Flips the buffer position and limit.
     */
    public void flip() {
        contents.flip();
    }

    /**
     * Clears the buffer position and limit.
     */
    public void clear() {
        contents.clear();
    }

    /**
     * Discards the buffer contents.
     */
    public void discard() {
        contents = EMPTY_BUFFER;
    }

    @Override
    public boolean readBoolean() {
        return contents.get() != 0;
    }

    @Override
    public byte readByte() {
        return contents.get();
    }

    @Override
    public int readUnsignedByte() {
        return contents.get() & 0xff;
    }

    @Override
    public short readShort() {
        return contents.getShort();
    }

    @Override
    public int readUnsignedShort() {
        return contents.getShort() & 0xffff;
    }

    @Override
    public char readChar() {
        return contents.getChar();
    }

    @Override
    public int readInt() {
        return contents.getInt();
    }

    @Override
    public long readLong() {
        return contents.getLong();
    }

    @Override
    public float readFloat() {
        return contents.getFloat();
    }

    @Override
    public double readDouble() {
        return contents.getDouble();
    }

    @Override
    public void readFully(byte[] b) {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        contents.get(b, off, len);
    }

    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() {
        try {
            return DataIoUtils.readUTF(this);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public int skipBytes(int n) {
        if (n <= 0) {
            return 0;
        }
        int skip = Math.min(n, contents.remaining());
        if (skip > 0) {
            contents.position(contents.position() + skip);
        }
        return skip;
    }

    @Override
    public void write(int b) {
        ensureWrite(Byte.BYTES);
        contents.put((byte) b);
    }

    @Override
    public void writeBoolean(boolean v) {
        ensureWrite(Byte.BYTES);
        contents.put(v ? (byte) 1 : (byte) 0);
    }

    @Override
    public void writeByte(int v) {
        ensureWrite(Byte.BYTES);
        contents.put((byte) v);
    }

    @Override
    public void writeShort(int v) {
        ensureWrite(Short.BYTES);
        contents.putShort((short) v);
    }

    @Override
    public void writeChar(int v) {
        ensureWrite(Character.BYTES);
        contents.putChar((char) v);
    }

    @Override
    public void writeInt(int v) {
        ensureWrite(Integer.BYTES);
        contents.putInt(v);
    }

    @Override
    public void writeLong(long v) {
        ensureWrite(Long.BYTES);
        contents.putLong(v);
    }

    @Override
    public void writeFloat(float v) {
        ensureWrite(Float.BYTES);
        contents.putFloat(v);
    }

    @Override
    public void writeDouble(double v) {
        ensureWrite(Double.BYTES);
        contents.putDouble(v);
    }

    @Override
    public void write(byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        ensureWrite(len);
        contents.put(b, off, len);
    }

    @Override
    public void writeBytes(String s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            writeByte(s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            writeChar(s.charAt(i));
        }
    }

    @Override
    public void writeUTF(String s) {
        try {
            DataIoUtils.writeUTF(this, s);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void ensureWrite(int bytes) {
        if (contents.remaining() < bytes) {
            int size = contents.limit();
            reallocate(size, size + bytes);
        }
    }

    private void reallocate(int currentSize, int requiredSize) {
        int newSize = computeExpansion(currentSize, requiredSize);
        if (LOG.isDebugEnabled()) {
            if (newSize > softLimit) {
                LOG.debug("softlimit exceeded: ({}->{})/{} ({})", currentSize, newSize, softLimit, this);
            } else if (LOG.isTraceEnabled()) {
                LOG.trace("expand buffer: ({}->{})/{} ({})", currentSize, newSize, softLimit, this);
            }
        }
        ByteBuffer newBuf = Buffers.allocate(newSize);
        newBuf.clear();

        ByteBuffer oldBuf = contents;
        oldBuf.flip();
        newBuf.put(oldBuf);

        this.contents = newBuf;
    }

    private int computeExpansion(int currentSize, int requiredSize) {
        if (requiredSize <= softLimit) {
            long nextSize = currentSize == 0 ? initial : (long) (currentSize * expansionFactor);
            long candidate = Math.max(nextSize, (long) requiredSize + BUFFER_MARGIN);
            return (int) Math.min(candidate, softLimit);
        } else {
            long nextSize = (long) (currentSize * LIMITED_EXPANSION_FACTOR);
            long candidate = Math.max(nextSize, (long) requiredSize + BUFFER_MARGIN);
            return (int) Math.min(candidate, Integer.MAX_VALUE);
        }
    }
}
