/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.vanilla.core.io;

import static java.nio.file.StandardOpenOption.*;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * An implementation of {@link DataReader} which just wraps {@link ReadableByteChannel}.
 * @since 0.4.0
 */
public class ByteChannelReader implements DataReader {

    private final ReadableByteChannel channel;

    private final ByteBuffer sizeBuffer = Buffers.allocate(Integer.BYTES);

    /**
     * Creates a new instance.
     * @param channel the source channel
     */
    public ByteChannelReader(ReadableByteChannel channel) {
        Arguments.requireNonNull(channel);
        this.channel = channel;
    }

    /**
     * Opens a file on the given path.
     * @param path the target path
     * @return the related writer
     * @throws IOException if I/O error was occurred while opening the file
     */
    public static DataReader open(Path path) throws IOException {
        Arguments.requireNonNull(path);
        return new ByteChannelReader(Files.newByteChannel(path, EnumSet.of(READ)));
    }

    @Override
    public int readInt() throws IOException {
        ByteBuffer buffer = sizeBuffer;
        buffer.clear();
        readFully(buffer);
        buffer.flip();
        return buffer.getInt();
    }

    @Override
    public void readFully(ByteBuffer destination) throws IOException {
        while (destination.hasRemaining()) {
            int read = channel.read(destination);
            if (read < 0) {
                throw new EOFException();
            }
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
