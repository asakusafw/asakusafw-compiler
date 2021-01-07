/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
import java.util.Random;
import java.util.function.IntConsumer;

import org.junit.Test;

import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.vanilla.core.testing.ShortPairSerDe;

/**
 * Test for {@link StreamGroupWriter}.
 */
public class StreamGroupWriterTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        List<Integer> inputs = new ArrayList<>();
        inputs.add(100);
        List<Integer> results = new ArrayList<>();
        try (StreamGroupWriter writer = new StreamGroupWriter(
                new MockStream(results::add),
                new ShortPairSerDe(), ShortPairSerDe.dataComparator(),
                1024, 100)) {
            for (Integer o : inputs) {
                writer.putObject(o);
            }
        }
        assertThat(results, is(sort(inputs)));
    }

    /**
     * w/o records.
     * @throws Exception if failed
     */
    @Test
    public void empty_records() throws Exception {
        List<Integer> results = new ArrayList<>();
        try (StreamGroupWriter writer = new StreamGroupWriter(
                new MockStream(results::add),
                new ShortPairSerDe(), ShortPairSerDe.dataComparator(),
                1024, 100)) {
            Lang.pass();
        }
        assertThat(results, hasSize(0));
    }

    /**
     * records should be sorted.
     * @throws Exception if failed
     */
    @Test
    public void sorted_key_diff() throws Exception {
        List<Integer> inputs = new ArrayList<>();
        inputs.add(0x0003_0007);
        inputs.add(0x0001_0009);
        inputs.add(0x0002_0008);
        List<Integer> results = new ArrayList<>();
        try (StreamGroupWriter writer = new StreamGroupWriter(
                new MockStream(results::add),
                new ShortPairSerDe(), ShortPairSerDe.dataComparator(),
                1024, 100)) {
            for (Integer o : inputs) {
                writer.putObject(o);
            }
        }
        assertThat(results, is(sort(inputs)));
    }

    /**
     * records should be sorted.
     * @throws Exception if failed
     */
    @Test
    public void sorted_key_common() throws Exception {
        List<Integer> inputs = new ArrayList<>();
        inputs.add(3);
        inputs.add(5);
        inputs.add(1);
        inputs.add(4);
        inputs.add(2);
        inputs.add(6);
        List<Integer> results = new ArrayList<>();
        try (StreamGroupWriter writer = new StreamGroupWriter(
                new MockStream(results::add),
                new ShortPairSerDe(), ShortPairSerDe.dataComparator(),
                1024, 100)) {
            for (Integer o : inputs) {
                writer.putObject(o);
            }
        }
        assertThat(results, is(sort(inputs)));
    }

    /**
     * records should be sorted.
     * @throws Exception if failed
     */
    @Test
    public void sorted_mixed() throws Exception {
        List<Integer> inputs = new ArrayList<>();
        inputs.add(0x0002_0006);
        inputs.add(0x0002_0004);
        inputs.add(0x0000_0001);
        inputs.add(0x0002_0005);
        inputs.add(0x0001_0003);
        inputs.add(0x0001_0002);
        List<Integer> results = new ArrayList<>();
        try (StreamGroupWriter writer = new StreamGroupWriter(
                new MockStream(results::add),
                new ShortPairSerDe(), ShortPairSerDe.dataComparator(),
                1024, 100)) {
            for (Integer o : inputs) {
                writer.putObject(o);
            }
        }
        assertThat(results, is(sort(inputs)));
    }

    /**
     * records should be sorted.
     * @throws Exception if failed
     */
    @Test
    public void flood() throws Exception {
        int chunks = 10;
        int threshold = 10_000;
        List<Integer> inputs = new ArrayList<>();
        Random rnd = new Random(6502);
        for (int i = 0, n = threshold * chunks; i < n; i++) {
            inputs.add(rnd.nextInt());
        }
        List<Integer> results = new ArrayList<>();
        try (StreamGroupWriter writer = new StreamGroupWriter(
                new MockStream(results::add),
                new ShortPairSerDe(), ShortPairSerDe.dataComparator(),
                1_000_001, threshold)) {
            for (Integer o : inputs) {
                writer.putObject(o);
            }
        }

        // kv pairs are sorted each 10,000 records
        for (int ci = 0; ci < chunks; ci++) {
            List<Integer> chunk = results.subList(ci * threshold, (ci + 1) * threshold);
            assertThat(chunk, is(sort(new ArrayList<>(chunk))));
        }
    }

    private static List<Integer> sort(List<Integer> values) {
        values.sort(ShortPairSerDe.comparator());
        return values;
    }

    private static class MockStream implements KeyValueSink.Stream {
        final IntConsumer sink;
        MockStream(IntConsumer sink) {
            this.sink = sink;
        }

        @Override
        public KeyValueSink offer(int recordCount, int keySize, int valueSize) throws IOException, InterruptedException {
            return new KeyValueSink() {
                private short lastKey = -1;
                @Override
                public void accept(ByteBuffer key, ByteBuffer value) throws IOException, InterruptedException {
                    assertThat(key.remaining(), is(2));
                    assertThat(value.remaining(), is(2));
                    lastKey = key.getShort();
                    put(value);
                }
                @Override
                public boolean accept(ByteBuffer value) throws IOException, InterruptedException {
                    assertThat(value.remaining(), is(2));
                    if (lastKey % 2 == 0) {
                        put(value);
                        return true;
                    } else {
                        return false;
                    }
                }
                private void put(ByteBuffer value) {
                    int record = (lastKey << Short.SIZE) | (value.getShort() & 0xffff);
                    sink.accept(record);
                }
                @Override
                public void close() throws IOException, InterruptedException {
                    return;
                }
            };
        }
    }
}
