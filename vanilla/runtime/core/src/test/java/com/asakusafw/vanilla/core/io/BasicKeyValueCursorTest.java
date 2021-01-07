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
import java.util.Arrays;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.asakusafw.vanilla.core.util.Buffers;

/**
 * Test for {@link BasicKeyValueCursor}.
 */
@RunWith(Parameterized.class)
public class BasicKeyValueCursorTest {

    /**
     * Returns the test parameters.
     * @return the test parameters
     */
    @Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { "basic", (Function<DataReader, KeyValueCursor>) BasicKeyValueCursor::new },
            { "direct", (Function<DataReader, KeyValueCursor>) r -> new DirectKeyValueCursor(r.getBuffer(), r) },
        });
    }

    private final Function<DataReader, KeyValueCursor> factory;

    /**
     * Creates a new instance.
     * @param id the parameter ID
     * @param factory the factory
     */
    public BasicKeyValueCursorTest(String id, Function<DataReader, KeyValueCursor> factory) {
        this.factory = factory;
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        int[][] groups = {
                {1, 100},
        };
        try (KeyValueCursor cursor = factory.apply(reader(groups))) {
            check(cursor, 1, 100);
            assertThat(cursor.next(), is(false));
        }
    }

    /**
     * w/o groups.
     * @throws Exception if failed
     */
    @Test
    public void empty_groups() throws Exception {
        int[][] groups = {
        };
        try (KeyValueCursor cursor = factory.apply(reader(groups))) {
            assertThat(cursor.next(), is(false));
        }
    }

    /**
     * w/ multiple groups.
     * @throws Exception if failed
     */
    @Test
    public void multiple_groups() throws Exception {
        int[][] groups = {
                {1, 100},
                {2, 200},
                {3, 300},
        };
        try (KeyValueCursor cursor = factory.apply(reader(groups))) {
            check(cursor, 1, 100);
            check(cursor, 2, 200);
            check(cursor, 3, 300);
            assertThat(cursor.next(), is(false));
        }
    }

    /**
     * w/ multiple records in group.
     * @throws Exception if failed
     */
    @Test
    public void multiple_records() throws Exception {
        int[][] groups = {
                {1, 100, 200, 300},
        };
        try (KeyValueCursor cursor = factory.apply(reader(groups))) {
            check(cursor, 1, 100);
            check(cursor, 1, 200);
            check(cursor, 1, 300);
            assertThat(cursor.next(), is(false));
        }
    }

    private static DataReader reader(int[][] groups) {
        ByteBuffer buffer = Buffers.allocate(65536);
        for (int[] group : groups) {
            buffer.putInt(Integer.BYTES);
            buffer.putInt(group[0]);
            for (int i = 1; i < group.length; i++) {
                buffer.putInt(Integer.BYTES);
                buffer.putInt(group[i]);
            }
            buffer.putInt(-1);
        }
        buffer.putInt(-1);
        buffer.flip();
        return new ByteBufferReader(buffer);
    }

    private static void check(KeyValueCursor cursor, int key, int value) throws IOException, InterruptedException {
        assertThat(cursor.next(), is(true));
        check(cursor.getKey(), key);
        check(cursor.getValue(), value);
    }

    private static void check(ByteBuffer buffer, int expected) {
        assertThat(buffer.getInt(), is(expected));
        assertThat(buffer.hasRemaining(), is(false));
    }
}
