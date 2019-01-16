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
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * A basic implementation of {@link KeyValueCursor} which can read contents written by {@link BasicKeyValueSink}.
 * @since 0.4.0
 */
public class BasicKeyValueCursor implements KeyValueCursor {

    private static final int INITIAL_BUFFER_SIZE = 256;

    private static final int BUFFER_MARGIN = 64;

    private final DataReader reader;

    private int lastKeySize = -1;

    private ByteBuffer keyBuffer;

    private ByteBuffer valueBuffer;

    private boolean sawEof = false;

    /**
     * Creates a new instance.
     * @param reader the source data reader
     */
    public BasicKeyValueCursor(DataReader reader) {
        Arguments.requireNonNull(reader);
        this.reader = reader;
        this.keyBuffer = Buffers.allocate(INITIAL_BUFFER_SIZE);
        this.valueBuffer = Buffers.allocate(INITIAL_BUFFER_SIZE);
    }

    /**
     * Creates a new instance.
     * @param reader the source data reader
     * @return the created instance
     */
    public static KeyValueCursor newInstance(DataReader reader) {
        Arguments.requireNonNull(reader);
        ByteBuffer buffer = reader.getBuffer();
        if (buffer == null) {
            return new BasicKeyValueCursor(reader);
        } else {
            return new DirectKeyValueCursor(Buffers.duplicate(buffer), reader);
        }
    }

    @Override
    public boolean next() throws IOException, InterruptedException {
        if (sawEof) {
            return false;
        }
        if (lastKeySize < 0) {
            // first time
            return nextHeadOfGroup();
        } else {
            DataReader r = reader;
            int sz = r.readInt();
            if (sz < 0) {
                // key break
                return nextHeadOfGroup();
            } else {
                // continue group
                assert lastKeySize >= 0;
                Buffers.range(keyBuffer, 0, lastKeySize);
                readValue(sz);
                return true;
            }
        }
    }

    private boolean nextHeadOfGroup() throws IOException, InterruptedException {
        int keySize = reader.readInt();
        if (keySize < 0) {
            sawEof = true;
            lastKeySize = -1;
            return false;
        }
        readKey(keySize);

        int valueSize = reader.readInt();
        Invariants.require(valueSize >= 0);
        readValue(valueSize);
        return true;
    }

    private void readKey(int size) throws IOException, InterruptedException {
        ByteBuffer buf = keyBuffer;
        if (buf == null || buf.capacity() < size) {
            buf = Buffers.allocate(size + BUFFER_MARGIN);
            keyBuffer = buf;
        }
        lastKeySize = size;
        Buffers.range(buf, 0, size);
        reader.readFully(buf);
        buf.flip();
    }

    private void readValue(int size) throws IOException, InterruptedException {
        ByteBuffer buf = valueBuffer;
        if (buf == null || buf.capacity() < size) {
            buf = Buffers.allocate(size + BUFFER_MARGIN);
            valueBuffer = buf;
        }
        Buffers.range(buf, 0, size);
        reader.readFully(buf);
        buf.flip();
    }

    @Override
    public ByteBuffer getKey() {
        return keyBuffer;
    }

    @Override
    public ByteBuffer getValue() {
        return valueBuffer;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        reader.close();
    }
}
