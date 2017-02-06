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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.asakusafw.lang.utils.common.Optionals;

/**
 * Test for {@link BasicBufferStore}.
 */
public class BasicBufferStoreTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        File directory;
        try (BasicBufferStore store = new BasicBufferStore()) {
            directory = store.getDirectory();
            try (DataReader.Provider entry = store.store(buffer("Hello, world!"))) {
                assertThat(read(entry), is("Hello, world!"));
                assertThat(read(entry), is("Hello, world!"));
            }
        }
        assertThat(directory.exists(), is(false));
    }

    /**
     * multiple entries.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        File directory;
        try (BasicBufferStore store = new BasicBufferStore()) {
            directory = store.getDirectory();
            try (DataReader.Provider c0 = store.store(buffer("Hello0"));
                    DataReader.Provider c1 = store.store(buffer("Hello1"));
                    DataReader.Provider c2 = store.store(buffer("Hello2"))) {
                assertThat(read(c0), is("Hello0"));
                assertThat(read(c1), is("Hello1"));
                assertThat(read(c2), is("Hello2"));
            }
        }
        assertThat(directory.exists(), is(false));
    }

    /**
     * w/ division.
     * @throws Exception if failed
     */
    @Test
    public void division() throws Exception {
        File directory;
        try (BasicBufferStore store = new BasicBufferStore(null, 2)) {
            directory = store.getDirectory();
            try (DataReader.Provider c0 = store.store(buffer("Hello0"));
                    DataReader.Provider c1 = store.store(buffer("Hello1"));
                    DataReader.Provider c2 = store.store(buffer("Hello2"));
                    DataReader.Provider c3 = store.store(buffer("Hello3"));
                    DataReader.Provider c4 = store.store(buffer("Hello4"))) {
                assertThat(read(c0), is("Hello0"));
                assertThat(read(c1), is("Hello1"));
                assertThat(read(c2), is("Hello2"));
                assertThat(read(c3), is("Hello3"));
                assertThat(read(c4), is("Hello4"));
            }
            long count = Optionals.of(directory.listFiles())
                .map(Arrays::asList)
                .orElse(Collections.emptyList())
                .stream()
                .peek(f -> assertThat(f.isDirectory(), is(true)))
                .count();
            assertThat(count, is(3L));
        }
        assertThat(directory.exists(), is(false));
    }
}
