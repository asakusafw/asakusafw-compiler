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
package com.asakusafw.lang.utils.buffer.nio;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.asakusafw.lang.utils.buffer.DataBuffer;
import com.asakusafw.lang.utils.buffer.DataIoUtils;

/**
 * Java NIO based implementation of {@link DataBuffer}.
 * @since 0.4.0
 */
public class NioDataBuffer implements DataBuffer {

    /**
     * An empty buffer (off-heap).
     */
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);

    /**
     * The target NIO buffer.
     */
    public ByteBuffer contents = EMPTY_BUFFER;

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
        contents.put((byte) b);
    }

    @Override
    public final void writeBoolean(boolean v) {
        contents.put(v ? (byte) 1 : (byte) 0);
    }

    @Override
    public final void writeByte(int v) {
        contents.put((byte) v);
    }

    @Override
    public final void writeShort(int v) {
        contents.putShort((short) v);
    }

    @Override
    public final void writeChar(int v) {
        contents.putChar((char) v);
    }

    @Override
    public final void writeInt(int v) {
        contents.putInt(v);
    }

    @Override
    public final void writeLong(long v) {
        contents.putLong(v);
    }

    @Override
    public final void writeFloat(float v) {
        contents.putFloat(v);
    }

    @Override
    public final void writeDouble(double v) {
        contents.putDouble(v);
    }

    @Override
    public final void write(byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public final void write(byte[] b, int off, int len) {
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
}
