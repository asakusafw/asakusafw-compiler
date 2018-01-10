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

import static com.asakusafw.vanilla.core.testing.BufferTestUtil.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

/**
 * Test for {@link SharedBuffer}.
 */
public class SharedBufferTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (Dummy data = new Dummy("Hello, world!")) {
            assertThat(data.isActive(), is(true));
            List<DataReader.Provider> shared = SharedBuffer.wrap(data, 2);
            try (DataReader.Provider s0 = shared.get(0);
                    DataReader.Provider s1 = shared.get(1)) {
                assertThat(read(s0), is("Hello, world!"));
                assertThat(read(s1), is("Hello, world!"));

                s0.close();
                assertThat(read(s1), is("Hello, world!"));
            }
            assertThat(data.isActive(), is(false));
        }
    }

    /**
     * share with 1.
     * @throws Exception if failed
     */
    @Test
    public void one() throws Exception {
        try (Dummy data = new Dummy("Hello, world!")) {
            assertThat(data.isActive(), is(true));
            List<DataReader.Provider> shared = SharedBuffer.wrap(data, 1);
            try (DataReader.Provider s0 = shared.get(0)) {
                assertThat(read(s0), is("Hello, world!"));
            }
            assertThat(data.isActive(), is(false));
        }
    }

    /**
     * w/ over close.
     * @throws Exception if failed
     */
    @Test
    public void over_close() throws Exception {
        try (Dummy data = new Dummy("Hello, world!")) {
            List<DataReader.Provider> shared = SharedBuffer.wrap(data, 2);
            try (DataReader.Provider s0 = shared.get(0);
                    DataReader.Provider s1 = shared.get(1)) {
                s0.close();
                s0.close();
                assertThat(read(s1), is("Hello, world!"));
            }
        }
    }

    private static class Dummy implements DataReader.Provider {

        private String contents;

        Dummy(String contents) {
            this.contents = contents;
        }

        public boolean isActive() {
            return contents != null;
        }

        @Override
        public DataReader open() throws IOException, InterruptedException {
            return new ByteBufferReader(buffer(contents));
        }

        @Override
        public void close() throws IOException, InterruptedException {
            contents = null;
        }
    }
}
