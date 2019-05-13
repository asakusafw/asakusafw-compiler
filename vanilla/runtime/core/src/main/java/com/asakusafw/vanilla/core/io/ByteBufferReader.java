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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * An implementation of {@link DataReader} which just wraps {@link ByteBuffer}.
 * @since 0.4.0
 */
public class ByteBufferReader implements DataReader {

    private ByteBuffer buffer;

    private final InterruptibleIo resource;

    /**
     * Creates a new instance.
     * @param buffer the internal buffer
     */
    public ByteBufferReader(ByteBuffer buffer) {
        this(buffer, null);
    }

    /**
     * Creates a new instance.
     * @param buffer the internal buffer
     * @param resource the attached resource (nullable)
     */
    public ByteBufferReader(ByteBuffer buffer, InterruptibleIo resource) {
        Arguments.requireNonNull(buffer);
        this.buffer = buffer;
        this.resource = resource;
    }

    @Override
    public ByteBuffer getBuffer() {
        Invariants.requireNonNull(buffer);
        return buffer;
    }

    @Override
    public int readInt() {
        Invariants.requireNonNull(buffer);
        return buffer.getInt();
    }

    @Override
    public void readFully(ByteBuffer destination) {
        Invariants.requireNonNull(buffer);
        Buffers.put(buffer, destination);
    }

    @Override
    public void close() throws IOException, InterruptedException {
        buffer = null;
        if (resource != null) {
            resource.close();
        }
    }
}
