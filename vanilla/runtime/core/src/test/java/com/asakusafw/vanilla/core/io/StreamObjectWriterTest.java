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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;

import org.junit.Test;

import com.asakusafw.dag.runtime.testing.IntSerDe;
import com.asakusafw.lang.utils.common.Lang;

/**
 * Test for {@link StreamObjectWriter}.
 */
public class StreamObjectWriterTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        List<Integer> results = new ArrayList<>();
        try (StreamObjectWriter writer = new StreamObjectWriter(
                new MockStream(results::add),
                new IntSerDe(),
                1024, 100)) {
            writer.putObject(10);
        }
        assertThat(results, contains(10));
    }

    /**
     * w/o records.
     * @throws Exception if failed
     */
    @Test
    public void empty_records() throws Exception {
        List<Integer> results = new ArrayList<>();
        try (StreamObjectWriter writer = new StreamObjectWriter(
                new MockStream(results::add),
                new IntSerDe(),
                1024, 100)) {
            Lang.pass();
        }
        assertThat(results, hasSize(0));
    }

    /**
     * flood buffer.
     * @throws Exception if failed
     */
    @Test
    public void flood_buffer() throws Exception {
        List<Integer> inputs = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            inputs.add(i * i % 100);
        }
        List<Integer> results = new ArrayList<>();
        try (StreamObjectWriter writer = new StreamObjectWriter(
                new MockStream(results::add),
                new IntSerDe(),
                100_000, 100_001)) {
            for (Integer o : inputs) {
                writer.putObject(o);
            }
        }
        assertThat(results, is(inputs));
    }

    /**
     * flood index.
     * @throws Exception if failed
     */
    @Test
    public void flood_records() throws Exception {
        List<Integer> inputs = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            inputs.add(i * i % 100);
        }
        List<Integer> results = new ArrayList<>();
        try (StreamObjectWriter writer = new StreamObjectWriter(
                new MockStream(results::add),
                new IntSerDe(),
                1_000_001, 10_000)) {
            for (Integer o : inputs) {
                writer.putObject(o);
            }
        }
        assertThat(results, is(inputs));
    }

    private static class MockStream implements RecordSink.Stream {
        final IntConsumer sink;
        MockStream(IntConsumer sink) {
            this.sink = sink;
        }

        @Override
        public RecordSink offer(int recordCount, int contentSize) throws IOException, InterruptedException {
            return new RecordSink() {
                @Override
                public void accept(ByteBuffer contents) throws IOException, InterruptedException {
                    sink.accept(contents.getInt());
                }
                @Override
                public void close() throws IOException, InterruptedException {
                    return;
                }
            };
        }
    }
}
