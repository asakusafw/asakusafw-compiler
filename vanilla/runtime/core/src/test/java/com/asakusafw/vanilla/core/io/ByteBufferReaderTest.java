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

import static com.asakusafw.vanilla.core.testing.BufferTestUtil.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * Test for {@link ByteBufferReader}.
 */
public class ByteBufferReaderTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (DataReader reader = new ByteBufferReader(buffer("Hello, world!"))) {
            assertThat(read(reader), is("Hello, world!"));
        }
    }

    /**
     * w/ direct buffer.
     * @throws Exception if failed
     */
    @Test
    public void direct() throws Exception {
        try (DataReader reader = new ByteBufferReader(buffer("Hello, world!"))) {
            ByteBuffer direct = reader.getBuffer();
            assertThat(direct, is(buffer("Hello, world!")));
        }
    }
}
