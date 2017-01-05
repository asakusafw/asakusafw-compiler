/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.utils.buffer.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.asakusafw.lang.utils.buffer.DataBuffer;
import com.asakusafw.lang.utils.buffer.DataIoUtils;

/**
 * A resizable {@link DataBuffer} implementation using Java NIO.
 * @since 0.4.0
 * @version 0.4.1
 */
public class ResizableNioDataBuffer implements DataBuffer {

    /**
     * An empty buffer (off-heap).
     * @since 0.4.1
     */
    public static final ByteBuffer EMPTY_BUFFER = allocate(0);

    static final double DEFAULT_BUFFER_EXPANSION_FACTOR = 1.5;

    static final double MINIMUM_BUFFER_EXPANSION_FACTOR = 1.1;

    static final int MINIMUM_EXPANSION_SIZE = 256;

    private final double expansionFactor;

    /**
     * The contents buffer.
     */
    public ByteBuffer contents = EMPTY_BUFFER;

    /**
     * Creates a new instance.
     */
    public ResizableNioDataBuffer() {
        this(0, DEFAULT_BUFFER_EXPANSION_FACTOR);
    }

    /**
     * Creates a new instance.
     * @param initialCapacity the initial buffer capacity in bytes
     * @param expansionFactor the buffer expansion factor
     */
    public ResizableNioDataBuffer(int initialCapacity, double expansionFactor) {
        this.contents = initialCapacity <= 0 ? EMPTY_BUFFER : allocate(initialCapacity);
        this.expansionFactor = Math.max(expansionFactor, MINIMUM_BUFFER_EXPANSION_FACTOR);
    }

    private static ByteBuffer allocate(int size) {
        return ByteBuffer.allocateDirect(size).order(ByteOrder.nativeOrder());
    }

    @Override
    public final boolean readBoolean() {
        return contents.get() != 0;
    }

    @Override
    public final byte readByte() {
        return contents.get();
    }

    @Override
    public final int readUnsignedByte() {
        return contents.get() & 0xff;
    }

    @Override
    public final short readShort() {
        return contents.getShort();
    }

    @Override
    public final int readUnsignedShort() {
        return contents.getShort() & 0xffff;
    }

    @Override
    public final char readChar() {
        return contents.getChar();
    }

    @Override
    public final int readInt() {
        return contents.getInt();
    }

    @Override
    public final long readLong() {
        return contents.getLong();
    }

    @Override
    public final float readFloat() {
        return contents.getFloat();
    }

    @Override
    public final double readDouble() {
        return contents.getDouble();
    }

    @Override
    public void readFully(byte[] b) {
        readFully(b, 0, b.length);
    }

    @Override
    public final void readFully(byte[] b, int off, int len) {
        contents.get(b, off, len);
    }

    @Override
    public final String readLine() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final String readUTF() {
        try {
            return DataIoUtils.readUTF(this);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public final int skipBytes(int n) {
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
    public final void write(int b) {
        ensureWrite(Byte.BYTES);
        contents.put((byte) b);
    }

    @Override
    public final void writeBoolean(boolean v) {
        ensureWrite(Byte.BYTES);
        contents.put(v ? (byte) 1 : (byte) 0);
    }

    @Override
    public final void writeByte(int v) {
        ensureWrite(Byte.BYTES);
        contents.put((byte) v);
    }

    @Override
    public final void writeShort(int v) {
        ensureWrite(Short.BYTES);
        contents.putShort((short) v);
    }

    @Override
    public final void writeChar(int v) {
        ensureWrite(Character.BYTES);
        contents.putChar((char) v);
    }

    @Override
    public final void writeInt(int v) {
        ensureWrite(Integer.BYTES);
        contents.putInt(v);
    }

    @Override
    public final void writeLong(long v) {
        ensureWrite(Long.BYTES);
        contents.putLong(v);
    }

    @Override
    public final void writeFloat(float v) {
        ensureWrite(Float.BYTES);
        contents.putFloat(v);
    }

    @Override
    public final void writeDouble(double v) {
        ensureWrite(Double.BYTES);
        contents.putDouble(v);
    }

    @Override
    public final void write(byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public final void write(byte[] b, int off, int len) {
        ensureWrite(len);
        contents.put(b, off, len);
    }

    @Override
    public final void writeBytes(String s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            writeByte(s.charAt(i));
        }
    }

    @Override
    public final void writeChars(String s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            writeChar(s.charAt(i));
        }
    }

    @Override
    public final void writeUTF(String s) {
        try {
            DataIoUtils.writeUTF(this, s);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void ensureWrite(int bytes) {
        if (contents.remaining() < bytes) {
            expand(contents.limit() + bytes);
        }
    }

    private void expand(int requiredBytes) {
        ByteBuffer oldBuf = contents;
        int expansion = Math.max((int) (oldBuf.limit() * expansionFactor), MINIMUM_EXPANSION_SIZE);
        int newSize = Math.max(expansion, requiredBytes);

        ByteBuffer newBuf = ByteBuffer.allocateDirect(newSize).order(oldBuf.order());
        newBuf.clear();

        oldBuf.flip();
        newBuf.put(oldBuf);

        this.contents = newBuf;
    }
}
