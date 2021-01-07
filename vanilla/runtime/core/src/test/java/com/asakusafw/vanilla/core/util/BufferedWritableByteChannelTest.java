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

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link BufferedWritableByteChannel}.
 */
public class BufferedWritableByteChannelTest {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * simple cast.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        File file = temporary.newFile();
        int size = 1_024_000;
        try (BufferedWritableByteChannel c = new BufferedWritableByteChannel(
                Files.newByteChannel(file.toPath(), StandardOpenOption.WRITE), 256)) {
            ByteBuffer buf = Buffers.allocate(1);
            for (int i = 0; i < size; i++) {
                buf.clear();
                buf.put((byte) i);
                buf.flip();
                int written = c.write(buf);
                assertEquals(written, 1);
                assertFalse(buf.hasRemaining());
            }
        }
        verify(file, size);
    }

    /**
     * w/ small buffer.
     * @throws Exception if failed
     */
    @Test
    public void small_buffer() throws Exception {
        File file = temporary.newFile();
        int size = 1_024_000;
        try (BufferedWritableByteChannel c = new BufferedWritableByteChannel(
                Files.newByteChannel(file.toPath(), StandardOpenOption.WRITE), 256)) {
            ByteBuffer buf = Buffers.allocate(65);
            writeAll(c, buf, size);
        }
        verify(file, size);
    }

    /**
     * w/ large buffer.
     * @throws Exception if failed
     */
    @Test
    public void large_buffer() throws Exception {
        File file = temporary.newFile();
        int size = 1_024_000;
        try (BufferedWritableByteChannel c = new BufferedWritableByteChannel(
                Files.newByteChannel(file.toPath(), StandardOpenOption.WRITE), 256)) {
            ByteBuffer buf = Buffers.allocate(1025);
            writeAll(c, buf, size);
        }
        verify(file, size);
    }

    private void writeAll(BufferedWritableByteChannel channel, ByteBuffer buffer, int size) throws IOException {
        int position = 0;
        while (position < size) {
            buffer.clear();
            while (position < size && buffer.hasRemaining()) {
                buffer.put((byte) position++);
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                int remain = buffer.remaining();
                int written = channel.write(buffer);
                assertEquals(buffer.remaining(), remain - written);
            }
        }
    }

    private void verify(File file, int size) throws IOException {
        int position = 0;
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
            while (true) {
                int c = in.read();
                if (c < 0) {
                    break;
                }
                assertEquals(c, position++ & 0xff);
            }
        }
        assertEquals(position, size);
    }
}
