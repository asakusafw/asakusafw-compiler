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
package com.asakusafw.vanilla.core.mirror;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.asakusafw.vanilla.core.io.ByteBufferWriter;
import com.asakusafw.vanilla.core.io.DataWriter;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * Mock implementation of output channel of {@link DataWriter}.
 */
public class MockDataChannel implements DataWriter.Channel {

    private final List<ByteBuffer> committed = new ArrayList<>();

    @Override
    public DataWriter acquire(int size) throws IOException, InterruptedException {
        return new ByteBufferWriter(Buffers.allocate(size));
    }

    @Override
    public void commit(DataWriter written) throws IOException, InterruptedException {
        ByteBufferWriter w = (ByteBufferWriter) written;
        ByteBuffer buffer = Buffers.duplicate(w.getBuffer());
        buffer.flip();
        committed.add(buffer);
    }

    /**
     * Returns the committed buffers.
     * @return the committed
     */
    public List<ByteBuffer> getCommitted() {
        return committed;
    }
}
