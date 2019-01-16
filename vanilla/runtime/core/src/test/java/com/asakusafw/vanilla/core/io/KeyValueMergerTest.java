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
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import com.asakusafw.vanilla.core.testing.ShortPairSerDe;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * Test for {@link KeyValueMerger}.
 */
public class KeyValueMergerTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        checkMerge(1, cursor(1));
    }

    /**
     * w/ multiple cursors.
     * @throws Exception if failed
     */
    @Test
    public void multiple_cursors() throws Exception {
        checkMerge(3, cursor(1), cursor(2), cursor(3));
    }

    /**
     * w/ multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple_records() throws Exception {
        checkMerge(6,
                cursor(0x0001_0001, 0x0002_0001),
                cursor(0x0001_0002, 0x0003_0002),
                cursor(0x0002_0003, 0x0003_0003));
    }

    /**
     * w/ random inputs.
     * @throws Exception if failed
     */
    @Test
    public void huge() throws Exception {
        int base = 100_000;
        checkMerge(base * 10,
                random(6502 + 1, base * 1),
                random(6502 + 2, base * 2),
                random(6502 + 3, base * 3),
                random(6502 + 4, base * 4));
    }

    private static void checkMerge(int records, KeyValueCursor... cursors) throws IOException, InterruptedException {
        try (KeyValueMerger merger = merger(cursors)) {
            check(merger, records);
        }
    }

    private static void check(KeyValueCursor cursor, int records) throws IOException, InterruptedException {
        int count = 0;
        ByteBuffer lastKey = Buffers.allocate(Short.BYTES);
        lastKey.put(Byte.MIN_VALUE);
        lastKey.put(Byte.MIN_VALUE);
        lastKey.flip();
        short lastValue = Short.MIN_VALUE;
        while (cursor.next()) {
            assertThat(cursor.getKey().remaining(), is(Short.BYTES));
            assertThat(cursor.getValue().remaining(), is(Short.BYTES));
            int keyDiff = lastKey.compareTo(cursor.getKey());
            if (keyDiff > 0) {
                fail(String.format("key %s must be <= %s", toKeyString(lastKey), toKeyString(cursor.getKey())));
            }
            if (keyDiff == 0) {
                assertThat(lastValue, is(lessThanOrEqualTo(toShort(cursor.getValue()))));
            }
            lastKey.put(cursor.getKey());
            lastKey.flip();
            lastValue = toShort(cursor.getValue());
            count++;
        }
        assertThat(count, is(records));
    }

    private static String toKeyString(ByteBuffer key) {
        key.mark();
        byte b0 = key.get();
        byte b1 = key.get();
        key.reset();
        return String.format("(%d, %d)", b0, b1);
    }

    private static short toShort(ByteBuffer value) {
        assertThat(value.remaining(), is(Short.BYTES));
        value.mark();
        short result = value.getShort();
        value.reset();
        return result;
    }

    private static KeyValueMerger merger(KeyValueCursor... cursors) {
        return new KeyValueMerger(Arrays.asList(cursors), ShortPairSerDe.dataComparator());
    }

    private static KeyValueCursor random(long seed, int count) {
        Random rnd = new Random(seed);
        Integer[] values = new Integer[count];
        for (int i = 0; i < count; i++) {
            values[i] = rnd.nextInt();
        }
        return cursor(Arrays.stream(values)
                .sorted(ShortPairSerDe.comparator())
                .mapToInt(i -> i)
                .toArray());
    }

    private static KeyValueCursor cursor(int... values) {
        return ShortPairSerDe.cursor(values, 0, values.length);
    }
}
