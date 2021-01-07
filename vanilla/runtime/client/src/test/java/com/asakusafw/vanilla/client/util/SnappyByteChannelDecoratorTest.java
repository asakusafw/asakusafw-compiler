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
package com.asakusafw.vanilla.client.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.vanilla.core.io.ByteChannelDecorator;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * Test for {@link SnappyByteChannelDecorator}.
 */
public class SnappyByteChannelDecoratorTest {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private final ByteChannelDecorator decorator = new SnappyByteChannelDecorator();

    private static final int FILE_SIZE = 1024 * 1024;

    private static final int DIVISOR = 256;

    private static final int SMALL_PRIME = 256 + 1;

    private static final int LARGE_PRIME = (256 * 1024) - 1;

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        test(1, 1);
    }

    /**
     * empty output.
     * @throws Exception if failed
     */
    @Test
    public void empty() throws Exception {
        test(0, DIVISOR);
    }

    /**
     * buffer size is divisor of file size.
     * @throws Exception if failed
     */
    @Test
    public void divisor() throws Exception {
        test(FILE_SIZE, DIVISOR);
    }

    /**
     * small buffer.
     * @throws Exception if failed
     */
    @Test
    public void small_buffer() throws Exception {
        test(FILE_SIZE, SMALL_PRIME);
    }

    /**
     * large buffer.
     * @throws Exception if failed
     */
    @Test
    public void large_buffer() throws Exception {
        test(FILE_SIZE, LARGE_PRIME);
    }

    private void test(int fileSize, int bufferSize) throws IOException, InterruptedException {
        ByteBuffer buffer = Buffers.allocate(bufferSize);
        Path file = temporary.newFile().toPath();
        try (WritableByteChannel c = decorator.decorate(
                (WritableByteChannel) Files.newByteChannel(file, StandardOpenOption.WRITE))) {
            write(c, buffer, fileSize);
        }
        try (ReadableByteChannel c = decorator.decorate(
                (ReadableByteChannel) Files.newByteChannel(file, StandardOpenOption.READ))) {
            verify(c, buffer, fileSize);
        }
    }

    private void write(WritableByteChannel channel, ByteBuffer buffer, int size) throws IOException {
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

    private void verify(ReadableByteChannel channel, ByteBuffer buffer, int size) throws IOException {
        int position = 0;
        while (true) {
            buffer.clear();
            int read = channel.read(buffer);
            if (read < 0) {
                break;
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                assertEquals(b, (byte) position++);
            }
        }
        assertEquals(position, size);
    }

}
