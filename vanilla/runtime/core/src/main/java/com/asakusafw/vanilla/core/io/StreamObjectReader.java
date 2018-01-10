/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import com.asakusafw.dag.api.common.Deserializer;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.lang.utils.buffer.nio.NioDataBuffer;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An implementation of {@link ObjectReader} using {@link RecordCursor}.
 * @since 0.4.0
 */
public class StreamObjectReader implements ObjectReader {

    private final RecordCursor.Stream cursors;

    private final Deserializer deserializer;

    private final NioDataBuffer wrapper = new NioDataBuffer();

    private RecordCursor current;

    private Object object;

    /**
     * Creates a new instance.
     * @param input the cursor stream
     * @param deserializer the object deserializer
     */
    public StreamObjectReader(RecordCursor.Stream input, Deserializer deserializer) {
        Arguments.requireNonNull(input);
        Arguments.requireNonNull(deserializer);
        this.cursors = input;
        this.deserializer = deserializer;
    }

    private boolean prepare() throws IOException, InterruptedException {
        while (true) {
            if (current == null) {
                RecordCursor next = cursors.poll();
                if (next == null) {
                    return false;
                }
                current = next;
            }
            if (current.next()) {
                return true;
            } else {
                closeCurrent();
            }
        }
    }

    @Override
    public boolean nextObject() throws IOException, InterruptedException {
        if (prepare()) {
            wrapper.contents = current.get();
            object = deserializer.deserialize(wrapper);
            wrapper.contents = NioDataBuffer.EMPTY_BUFFER;
            return true;
        } else {
            object = null;
            return false;
        }
    }

    @Override
    public Object getObject() throws IOException, InterruptedException {
        return object;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closeCurrent();
    }

    private void closeCurrent() throws IOException, InterruptedException {
        if (current != null) {
            current.close();
            current = null;
        }
    }
}
