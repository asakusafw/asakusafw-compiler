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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.lang.utils.common.Tuple;
import com.asakusafw.vanilla.core.mirror.MockDataChannel;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * Test for {@link BasicKeyValueSink}.
 */
public class BasicKeyValueSinkTest {

    private final MockDataChannel channel = new MockDataChannel();

    private final KeyValueSink.Stream stream = BasicKeyValueSink.stream(channel);

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (KeyValueSink sink = stream.offer(1, Integer.BYTES * 1, bytes("Hello, world!").length)) {
            put(sink, 1, "Hello, world!");
        }
        assertThat(committed(), is(Arrays.asList(
                new Tuple<>(1, "Hello, world!"))));
    }

    /**
     * w/o records.
     * @throws Exception if failed
     */
    @Test
    public void empty_records() throws Exception {
        try (KeyValueSink sink = stream.offer(1, 0, 0)) {
            Lang.pass();
        }
        assertThat(committed(), hasSize(0));
    }

    /**
     * w/ multiple groups.
     * @throws Exception if failed
     */
    @Test
    public void multiple_group() throws Exception {
        try (KeyValueSink sink = stream.offer(3, Integer.BYTES * 3, bytes("HelloN").length * 3)) {
            put(sink, 1, "Hello1");
            put(sink, 2, "Hello2");
            put(sink, 3, "Hello3");
        }
        assertThat(committed(), is(Arrays.asList(
                new Tuple<>(1, "Hello1"),
                new Tuple<>(2, "Hello2"),
                new Tuple<>(3, "Hello3"))));
    }

    /**
     * w/ multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple_records() throws Exception {
        try (KeyValueSink sink = stream.offer(3, Integer.BYTES * 3, bytes("HelloN").length * 3)) {
            put(sink, 1, "Hello1");
            put(sink, "Hello2");
            put(sink, "Hello3");
        }
        assertThat(committed(), is(Arrays.asList(
                new Tuple<>(1, "Hello1"),
                new Tuple<>(1, "Hello2"),
                new Tuple<>(1, "Hello3"))));
    }

    private static void put(KeyValueSink sink, int key, String value) throws IOException, InterruptedException {
        byte[] bytes = bytes(value);
        ByteBuffer kBuf = Buffers.allocate(Integer.BYTES);
        ByteBuffer vBuf = Buffers.allocate(bytes.length);
        kBuf.putInt(key).flip();
        vBuf.put(bytes).flip();
        sink.accept(kBuf, vBuf);
    }

    private static void put(KeyValueSink sink, String value) throws IOException, InterruptedException {
        byte[] bytes = bytes(value);
        ByteBuffer vBuf = Buffers.allocate(bytes.length);
        vBuf.put(bytes).flip();
        Invariants.require(sink.accept(vBuf));
    }

    private List<Tuple<Integer, String>> committed() throws IOException, InterruptedException {
        List<Tuple<Integer, String>> results = new ArrayList<>();
        for (ByteBuffer buffer : channel.getCommitted()) {
            try (KeyValueCursor cursor = new BasicKeyValueCursor(new ByteBufferReader(buffer))) {
                while (cursor.next()) {
                    int key = cursor.getKey().getInt();
                    String value = string(cursor.getValue());
                    results.add(new Tuple<>(key, value));
                }
            }
        }
        return results;
    }
}
