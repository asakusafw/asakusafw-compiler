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

import static com.asakusafw.vanilla.core.testing.BufferTestUtil.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.asakusafw.vanilla.core.util.Buffers;

/**
 * Test for {@link ByteBufferWriter}.
 */
public class ByteBufferWriterTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        ByteBuffer buffer = Buffers.allocate(1024);
        try (DataWriter writer = new ByteBufferWriter(buffer)) {
            write(writer, "Hello, world!");
        }
        buffer.flip();
        assertThat(read(buffer), is("Hello, world!"));
    }

    /**
     * w/ direct buffer.
     * @throws Exception if failed
     */
    @Test
    public void direct() throws Exception {
        ByteBuffer buffer = Buffers.allocate(1024);
        try (DataWriter writer = new ByteBufferWriter(buffer)) {
            writer.getBuffer().put(buffer("Hello, world!"));
        }
        buffer.flip();
        assertThat(read(buffer), is("Hello, world!"));
    }
}
