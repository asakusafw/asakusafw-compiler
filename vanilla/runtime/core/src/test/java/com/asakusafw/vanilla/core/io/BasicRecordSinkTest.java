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
import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.vanilla.core.mirror.MockDataChannel;

/**
 * Test for {@link BasicRecordSink}.
 */
public class BasicRecordSinkTest {

    private final MockDataChannel channel = new MockDataChannel();

    private final RecordSink.Stream stream = BasicRecordSink.stream(channel);

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (RecordSink sink = stream.offer(1, bytes("Hello, world!").length)) {
            put(sink, "Hello, world!");
        }
        assertThat(committed(), contains("Hello, world!"));
    }

    /**
     * w/o records.
     * @throws Exception if failed
     */
    @Test
    public void empty_records() throws Exception {
        try (RecordSink sink = stream.offer(1, 0)) {
            Lang.pass();
        }
        assertThat(committed(), hasSize(0));
    }

    /**
     * w/ multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        try (RecordSink sink = stream.offer(3, bytes("HelloN").length * 3)) {
            put(sink, "Hello1");
            put(sink, "Hello2");
            put(sink, "Hello3");
        }
        assertThat(committed(), contains("Hello1", "Hello2", "Hello3"));
    }

    private static void put(RecordSink sink, String string) throws IOException, InterruptedException {
        sink.accept(ByteBuffer.wrap(bytes(string)));
    }

    private List<String> committed() throws IOException, InterruptedException {
        List<String> results = new ArrayList<>();
        for (ByteBuffer buffer : channel.getCommitted()) {
            try (RecordCursor cursor = new BasicRecordCursor(new ByteBufferReader(buffer))) {
                while (cursor.next()) {
                    results.add(string(cursor.get()));
                }
            }
        }
        return results;
    }
}
