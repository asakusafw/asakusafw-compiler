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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * A zero-copy implementation of {@link KeyValueCursor} which can read contents written by {@link BasicKeyValueSink}.
 * @since 0.4.0
 */
public class DirectKeyValueCursor implements KeyValueCursor {

    private ByteBuffer buffer;

    private ByteBuffer keyView;

    private ByteBuffer valueView;

    private int lastKeyBegin = -1;

    private int lastKeyEnd = -1;

    private final InterruptibleIo resource;

    /**
     * Creates a new instance.
     * @param buffer the source buffer
     * @param resource the attached resource (nullable)
     */
    public DirectKeyValueCursor(ByteBuffer buffer, InterruptibleIo resource) {
        Arguments.requireNonNull(buffer);
        this.buffer = Buffers.duplicate(buffer);
        this.keyView = Buffers.duplicate(buffer);
        this.valueView = Buffers.duplicate(buffer);
        this.resource = resource;
    }

    @Override
    public boolean next() throws IOException, InterruptedException {
        if (buffer == null) {
            return false;
        }
        if (lastKeyEnd < 0) {
            // first time
            return nextHeadOfGroup();
        } else {
            ByteBuffer buf = buffer;
            int size = buf.getInt();
            if (size < 0) {
                // key break
                return nextHeadOfGroup();
            } else {
                // continue group
                assert lastKeyEnd >= 0;
                Buffers.range(keyView, lastKeyBegin, lastKeyEnd);
                readValue(size);
                return true;
            }
        }
    }

    private boolean nextHeadOfGroup() {
        int keySize = buffer.getInt();
        if (keySize < 0) {
            release();
            return false;
        }
        readKey(keySize);

        int valueSize = buffer.getInt();
        Invariants.require(valueSize >= 0);
        readValue(valueSize);
        return true;
    }

    private void readKey(int size) {
        int begin = buffer.position();
        int end = begin + size;
        buffer.position(end);
        Buffers.range(keyView, begin, end);
        this.lastKeyBegin = begin;
        this.lastKeyEnd = end;
    }

    private void readValue(int size) {
        int begin = buffer.position();
        int end = begin + size;
        buffer.position(end);
        Buffers.range(valueView, begin, end);
    }

    @Override
    public ByteBuffer getKey() {
        return keyView;
    }

    @Override
    public ByteBuffer getValue() {
        return valueView;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        release();
        if (resource != null) {
            resource.close();
        }
    }

    private void release() {
        buffer = null;
        keyView = null;
        valueView = null;
        lastKeyBegin = -1;
        lastKeyEnd = -1;
    }
}
