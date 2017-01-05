/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * A basic implementation of {@link RecordCursor} which can read contents written by {@link BasicRecordSink}.
 * @since 0.4.0
 */
public class BasicRecordCursor implements RecordCursor {

    private static final int INITIAL_BUFFER_SIZE = 256;

    private static final int BUFFER_MARGIN = 64;

    private final DataReader reader;

    private ByteBuffer buffer;

    /**
     * Creates a new instance.
     * @param reader the source data reader
     */
    public BasicRecordCursor(DataReader reader) {
        Arguments.requireNonNull(reader);
        this.reader = reader;
        this.buffer = Buffers.allocate(INITIAL_BUFFER_SIZE);
    }

    /**
     * Creates a new instance.
     * @param reader the source data reader
     * @return the created instance
     */
    public static RecordCursor newInstance(DataReader reader) {
        Arguments.requireNonNull(reader);
        ByteBuffer buffer = reader.getBuffer();
        if (buffer == null) {
            return new BasicRecordCursor(reader);
        } else {
            return new DirectRecordCursor(Buffers.duplicate(buffer), reader);
        }
    }

    @Override
    public boolean next() throws IOException, InterruptedException {
        if (buffer == null) {
            return false;
        }
        DataReader r = reader;
        int size = r.readInt();
        if (size < 0) {
            buffer = null;
            return false;
        } else {
            ByteBuffer buf = buffer;
            if (buf.capacity() < size) {
                buf = Buffers.allocate(size + BUFFER_MARGIN);
                buffer = buf;
            }
            Buffers.range(buf, 0, size);
            r.readFully(buf);
            buf.flip();
            return true;
        }
    }

    @Override
    public ByteBuffer get() {
        return buffer;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        reader.close();
    }
}
