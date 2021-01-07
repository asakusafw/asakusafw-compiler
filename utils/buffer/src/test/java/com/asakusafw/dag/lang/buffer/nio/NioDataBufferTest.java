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
package com.asakusafw.dag.lang.buffer.nio;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;

import com.asakusafw.lang.utils.buffer.nio.NioDataBuffer;

/**
 * Test for {@link NioDataBuffer}.
 */
public class NioDataBufferTest {

    /**
     * boolean.
     */
    @Test
    public void io_boolean() {
        Buf buf = new Buf(64);

        buf.writeBoolean(true);
        buf.writeBoolean(false);
        assertThat(buf.offset(), is(2));
        buf.rewind();

        assertThat(buf.readBoolean(), is(true));
        assertThat(buf.readBoolean(), is(false));
        assertThat(buf.offset(), is(2));
    }

    /**
     * byte.
     */
    @Test
    public void io_byte() {
        Buf buf = new Buf(64);

        buf.writeByte(100);
        assertThat(buf.offset(), is(Byte.BYTES));
        buf.rewind();

        assertThat(buf.readByte(), is((byte) 100));
        assertThat(buf.offset(), is(Byte.BYTES));
    }

    /**
     * short.
     */
    @Test
    public void io_short() {
        Buf buf = new Buf(64);

        buf.writeShort(100);
        assertThat(buf.offset(), is(Short.BYTES));
        buf.rewind();

        assertThat(buf.readShort(), is((short) 100));
        assertThat(buf.offset(), is(Short.BYTES));
    }

    /**
     * char.
     */
    @Test
    public void io_char() {
        Buf buf = new Buf(64);

        buf.writeChar('A');
        assertThat(buf.offset(), is(Character.BYTES));
        buf.rewind();

        assertThat(buf.readChar(), is('A'));
        assertThat(buf.offset(), is(Character.BYTES));
    }

    /**
     * int.
     */
    @Test
    public void io_int() {
        Buf buf = new Buf(64);

        buf.writeInt(100);
        assertThat(buf.offset(), is(Integer.BYTES));
        buf.rewind();

        assertThat(buf.readInt(), is(100));
        assertThat(buf.offset(), is(Integer.BYTES));
    }

    /**
     * long.
     */
    @Test
    public void io_long() {
        Buf buf = new Buf(64);

        buf.writeLong(100);
        assertThat(buf.offset(), is(Long.BYTES));
        buf.rewind();

        assertThat(buf.readLong(), is(100L));
        assertThat(buf.offset(), is(Long.BYTES));
    }

    /**
     * float.
     */
    @Test
    public void io_float() {
        Buf buf = new Buf(64);

        buf.writeFloat(100);
        assertThat(buf.offset(), is(Float.BYTES));
        buf.rewind();

        assertThat(buf.readFloat(), is(100f));
        assertThat(buf.offset(), is(Float.BYTES));
    }

    /**
     * double.
     */
    @Test
    public void io_double() {
        Buf buf = new Buf(64);

        buf.writeDouble(100);
        assertThat(buf.offset(), is(Double.BYTES));
        buf.rewind();

        assertThat(buf.readDouble(), is(100d));
        assertThat(buf.offset(), is(Double.BYTES));
    }

    /**
     * byte array.
     */
    @Test
    public void io_byte_array() {
        Buf buf = new Buf(64);

        byte[] src = new byte[] { 1, 2, 3, 4, };
        buf.write(src);
        assertThat(buf.offset(), is(4));
        buf.rewind();

        byte[] dst = new byte[4];
        buf.readFully(dst);
        assertThat(dst, is(src));
        assertThat(buf.offset(), is(4));
    }

    /**
     * unsigned byte.
     */
    @Test
    public void io_ubyte() {
        Buf buf = new Buf(64);

        buf.writeByte(0xff);
        buf.rewind();

        assertThat(buf.readUnsignedByte(), is(0xff));
        assertThat(buf.offset(), is(Byte.BYTES));
    }

    /**
     * unsigned short.
     */
    @Test
    public void io_ushort() {
        Buf buf = new Buf(64);

        buf.writeShort(0xffff);
        buf.rewind();

        assertThat(buf.readUnsignedShort(), is(0xffff));
        assertThat(buf.offset(), is(Short.BYTES));
    }

    /**
     * UTF.
     */
    @Test
    public void io_utf() {
        Buf buf = new Buf(64);

        buf.writeUTF("Hello, world!");
        int offset = buf.offset();
        buf.rewind();

        assertThat(buf.readUTF(), is("Hello, world!"));
        assertThat(buf.offset(), is(offset));
    }

    /**
     * string as bytes.
     */
    @Test
    public void io_string_bytes() {
        Buf buf = new Buf(64);

        buf.writeBytes("Hello!");
        assertThat(buf.offset(), is("Hello!".length() * Byte.BYTES));
        buf.rewind();

        assertThat(buf.readByte(), is((byte) 'H'));
        assertThat(buf.readByte(), is((byte) 'e'));
        assertThat(buf.readByte(), is((byte) 'l'));
        assertThat(buf.readByte(), is((byte) 'l'));
        assertThat(buf.readByte(), is((byte) 'o'));
        assertThat(buf.readByte(), is((byte) '!'));
    }

    /**
     * string as chars.
     */
    @Test
    public void io_string_chars() {
        Buf buf = new Buf(64);

        buf.writeChars("Hello!");
        assertThat(buf.offset(), is("Hello!".length() * Character.BYTES));
        buf.rewind();

        assertThat(buf.readChar(), is('H'));
        assertThat(buf.readChar(), is('e'));
        assertThat(buf.readChar(), is('l'));
        assertThat(buf.readChar(), is('l'));
        assertThat(buf.readChar(), is('o'));
        assertThat(buf.readChar(), is('!'));
    }

    /**
     * skip.
     */
    @Test
    public void skip() {
        Buf buf = new Buf(64);

        buf.writeInt(100);
        buf.writeInt(200);
        buf.writeInt(300);
        buf.writeInt(400);
        buf.rewind();

        assertThat(buf.skipBytes(Integer.BYTES), is(Integer.BYTES));
        assertThat(buf.readInt(), is(200));
        assertThat(buf.skipBytes(Integer.BYTES), is(Integer.BYTES));
        assertThat(buf.readInt(), is(400));
        assertThat(buf.offset(), is(4 * Integer.BYTES));
    }

    private static final class Buf extends NioDataBuffer {

        public Buf(int size) {
            this.contents = ByteBuffer.allocate(size).order(ByteOrder.nativeOrder());
        }

        public void rewind() {
            this.contents.clear();
        }

        public int offset() {
            return contents.position();
        }
    }
}
