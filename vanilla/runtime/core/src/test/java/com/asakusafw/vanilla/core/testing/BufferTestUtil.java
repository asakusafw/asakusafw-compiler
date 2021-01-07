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
package com.asakusafw.vanilla.core.testing;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import com.asakusafw.vanilla.core.io.DataReader;
import com.asakusafw.vanilla.core.io.DataWriter;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * Utilities of buffers.
 */
public final class BufferTestUtil {

    private BufferTestUtil() {
        return;
    }

    /**
     * Returns a new byte array that contains the given string value.
     * @param value the value
     * @return the created byte array
     */
    public static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Returns a string in the buffer.
     * @param buffer the buffer
     * @return the string
     */
    public static String string(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Returns a new buffer that contains the given string value.
     * @param value the value
     * @return the created buffer
     */
    public static ByteBuffer buffer(String value) {
        byte[] bytes = bytes(value);
        ByteBuffer b = Buffers.allocate(Integer.BYTES * 2 + bytes.length);
        b.putInt(bytes.length);
        b.put(bytes);
        b.putInt(-1);
        b.flip();
        return b;
    }

    /**
     * Reads a buffer created in {@link #buffer(String)}.
     * @param provider the provider
     * @return the contents
     * @throws IOException if I/O error was occurred while reading contents
     * @throws InterruptedException if interrupted while reading contents
     */
    public static String read(DataReader.Provider provider) throws IOException, InterruptedException {
        try (DataReader r = provider.open()) {
            return read(r);
        }
    }

    /**
     * Reads a buffer created in {@link #buffer(String)}.
     * @param reader the reader
     * @return the contents
     * @throws IOException if I/O error was occurred while reading contents
     * @throws InterruptedException if interrupted while reading contents
     */
    public static String read(DataReader reader) throws IOException, InterruptedException {
        int size = reader.readInt();
        ByteBuffer b = Buffers.allocate(size);
        reader.readFully(b);
        assertThat(reader.readInt(), is(-1));

        b.flip();
        return string(b);
    }

    /**
     * Reads a buffer created in {@link #buffer(String)}.
     * @param buffer the reader
     * @return the contents
     */
    public static String read(ByteBuffer buffer) {
        int size = buffer.getInt();
        ByteBuffer slice = buffer.duplicate();
        slice.limit(slice.position() + size);
        buffer.position(slice.limit());
        assertThat(buffer.getInt(), is(-1));
        return string(slice);
    }

    /**
     * Reads a buffer created in {@link #buffer(String)}.
     * @param writer the destination writer
     * @param value the value
     * @throws IOException if I/O error was occurred while reading contents
     * @throws InterruptedException if interrupted while reading contents
     */
    public static void write(DataWriter writer, String value) throws IOException, InterruptedException {
        byte[] bytes = bytes(value);
        writer.writeInt(bytes.length);
        writer.writeFully(ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()));
        writer.writeInt(-1);
    }
}
