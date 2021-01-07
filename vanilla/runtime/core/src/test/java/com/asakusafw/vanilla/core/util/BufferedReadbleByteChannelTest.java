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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link BufferedReadbleByteChannel}.
 */
public class BufferedReadbleByteChannelTest {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        int size = 1_024_000;
        File file = create(size);
        try (BufferedReadbleByteChannel c = new BufferedReadbleByteChannel(
                Files.newByteChannel(file.toPath(), StandardOpenOption.READ), 256)) {
            ByteBuffer buf = Buffers.allocate(1);
            for (int i = 0; i < size; i++) {
                buf.clear();
                int read = c.read(buf);
                assertEquals(read, 1);
                assertFalse(buf.hasRemaining());
                buf.flip();
                assertEquals(buf.get(), (byte) i);
            }

            buf.clear();
            int read = c.read(buf);
            assertEquals(read, -1);
            assertTrue(buf.hasRemaining());
        }
    }

    /**
     * w/ small buffer.
     * @throws Exception if failed
     */
    @Test
    public void small_buffer() throws Exception {
        int size = 1_024_000;
        File file = create(size);
        try (BufferedReadbleByteChannel c = new BufferedReadbleByteChannel(
                Files.newByteChannel(file.toPath(), StandardOpenOption.READ), 256)) {
            ByteBuffer buf = Buffers.allocate(99);
            int position = readAll(c, buf);
            assertEquals(position, size);
        }
    }

    /**
     * w/ large buffer.
     * @throws Exception if failed
     */
    @Test
    public void large_buffer() throws Exception {
        int size = 1_024_000;
        File file = create(size);
        try (BufferedReadbleByteChannel c = new BufferedReadbleByteChannel(
                Files.newByteChannel(file.toPath(), StandardOpenOption.READ), 256)) {
            ByteBuffer buf = Buffers.allocate(1025);
            int position = readAll(c, buf);
            assertEquals(position, size);
        }
    }

    private static int readAll(BufferedReadbleByteChannel channel, ByteBuffer buffer) throws IOException {
        int position = 0;
        while (true) {
            buffer.clear();
            int read = channel.read(buffer);
            if (read == -1) {
                break;
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                assertEquals(buffer.get(), (byte) position++);
            }
        }
        return position;
    }

    private File create(int size) throws IOException {
        File file = temporary.newFile();
        try (OutputStream w = new BufferedOutputStream(new FileOutputStream(file))) {
            for (int i = 0; i < size; i++) {
                w.write(i);
            }
        }
        return file;
    }
}
