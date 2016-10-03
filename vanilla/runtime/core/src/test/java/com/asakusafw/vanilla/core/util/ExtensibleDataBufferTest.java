/**
 * Copyright 2011-2016 Asakusa Framework Team.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Test for {@link ExtensibleDataBuffer}.
 */
public class ExtensibleDataBufferTest {

    /**
     * boolean.
     */
    @Test
    public void io_boolean() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeBoolean(true);
        buf.writeBoolean(false);
        assertThat(buf.position(), is(2));
        buf.clear();

        assertThat(buf.readBoolean(), is(true));
        assertThat(buf.readBoolean(), is(false));
        assertThat(buf.position(), is(2));
    }

    /**
     * byte.
     */
    @Test
    public void io_byte() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeByte(100);
        assertThat(buf.position(), is(Byte.BYTES));
        buf.clear();

        assertThat(buf.readByte(), is((byte) 100));
        assertThat(buf.position(), is(Byte.BYTES));
    }

    /**
     * short.
     */
    @Test
    public void io_short() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeShort(100);
        assertThat(buf.position(), is(Short.BYTES));
        buf.clear();

        assertThat(buf.readShort(), is((short) 100));
        assertThat(buf.position(), is(Short.BYTES));
    }

    /**
     * char.
     */
    @Test
    public void io_char() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeChar('A');
        assertThat(buf.position(), is(Character.BYTES));
        buf.clear();

        assertThat(buf.readChar(), is('A'));
        assertThat(buf.position(), is(Character.BYTES));
    }

    /**
     * int.
     */
    @Test
    public void io_int() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeInt(100);
        assertThat(buf.position(), is(Integer.BYTES));
        buf.clear();

        assertThat(buf.readInt(), is(100));
        assertThat(buf.position(), is(Integer.BYTES));
    }

    /**
     * long.
     */
    @Test
    public void io_long() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeLong(100);
        assertThat(buf.position(), is(Long.BYTES));
        buf.clear();

        assertThat(buf.readLong(), is(100L));
        assertThat(buf.position(), is(Long.BYTES));
    }

    /**
     * float.
     */
    @Test
    public void io_float() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeFloat(100);
        assertThat(buf.position(), is(Float.BYTES));
        buf.clear();

        assertThat(buf.readFloat(), is(100f));
        assertThat(buf.position(), is(Float.BYTES));
    }

    /**
     * double.
     */
    @Test
    public void io_double() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeDouble(100);
        assertThat(buf.position(), is(Double.BYTES));
        buf.clear();

        assertThat(buf.readDouble(), is(100d));
        assertThat(buf.position(), is(Double.BYTES));
    }

    /**
     * byte array.
     */
    @Test
    public void io_byte_array() {
        ExtensibleDataBuffer buf = allocate();

        byte[] src = new byte[] { 1, 2, 3, 4, };
        buf.write(src);
        assertThat(buf.position(), is(4));
        buf.clear();

        byte[] dst = new byte[4];
        buf.readFully(dst);
        assertThat(dst, is(src));
        assertThat(buf.position(), is(4));
    }

    /**
     * unsigned byte.
     */
    @Test
    public void io_ubyte() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeByte(0xff);
        buf.clear();

        assertThat(buf.readUnsignedByte(), is(0xff));
        assertThat(buf.position(), is(Byte.BYTES));
    }

    /**
     * unsigned short.
     */
    @Test
    public void io_ushort() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeShort(0xffff);
        buf.clear();

        assertThat(buf.readUnsignedShort(), is(0xffff));
        assertThat(buf.position(), is(Short.BYTES));
    }

    /**
     * UTF.
     */
    @Test
    public void io_utf() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeUTF("Hello, world!");
        int offset = buf.position();
        buf.clear();

        assertThat(buf.readUTF(), is("Hello, world!"));
        assertThat(buf.position(), is(offset));
    }

    /**
     * string as bytes.
     */
    @Test
    public void io_string_bytes() {
        ExtensibleDataBuffer buf = allocate();

        buf.writeBytes("Hello!");
        assertThat(buf.position(), is("Hello!".length() * Byte.BYTES));
        buf.clear();

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
        ExtensibleDataBuffer buf = allocate();

        buf.writeChars("Hello!");
        assertThat(buf.position(), is("Hello!".length() * Character.BYTES));
        buf.clear();

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
        ExtensibleDataBuffer buf = allocate();

        buf.writeInt(100);
        buf.writeInt(200);
        buf.writeInt(300);
        buf.writeInt(400);
        buf.clear();

        assertThat(buf.skipBytes(Integer.BYTES), is(Integer.BYTES));
        assertThat(buf.readInt(), is(200));
        assertThat(buf.skipBytes(Integer.BYTES), is(Integer.BYTES));
        assertThat(buf.readInt(), is(400));
        assertThat(buf.position(), is(4 * Integer.BYTES));
    }

    /**
     * w/ soft limit.
     */
    @Test
    public void soft_limit() {
        int count = 10_000;
        ExtensibleDataBuffer buf = allocate(count * Integer.BYTES);
        for (int i = 0; i < count; i++) {
            buf.writeInt(i);
        }
        assertThat(buf.buffer().capacity(), is(count * Integer.BYTES));

        // can exceed soft limit
        buf.writeInt(0);
    }

    private ExtensibleDataBuffer allocate() {
        return new ExtensibleDataBuffer(4 * 1024, 16 * 1024);
    }

    private ExtensibleDataBuffer allocate(int limit) {
        return new ExtensibleDataBuffer(0, limit);
    }
}
