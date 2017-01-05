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
 * Test for {@link BasicRecordCursor}.
 */
@RunWith(Parameterized.class)
public class BasicRecordCursorTest {

    /**
     * Returns the test parameters.
     * @return the test parameters
     */
    @Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { "basic", (Function<DataReader, RecordCursor>) BasicRecordCursor::new },
            { "direct", (Function<DataReader, RecordCursor>) r -> new DirectRecordCursor(r.getBuffer(), r) },
        });
    }

    private final Function<DataReader, RecordCursor> factory;

    /**
     * Creates a new instance.
     * @param id the parameter ID
     * @param factory the factory
     */
    public BasicRecordCursorTest(String id, Function<DataReader, RecordCursor> factory) {
        this.factory = factory;
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (RecordCursor cursor = factory.apply(reader(100))) {
            check(cursor, 100);
            assertThat(cursor.next(), is(false));
        }
    }

    /**
     * w/o records.
     * @throws Exception if failed
     */
    @Test
    public void empty_records() throws Exception {
        try (RecordCursor cursor = factory.apply(reader())) {
            assertThat(cursor.next(), is(false));
        }
    }

    /**
     * w/ multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        try (RecordCursor cursor = factory.apply(reader(100, 200, 300))) {
            check(cursor, 100);
            check(cursor, 200);
            check(cursor, 300);
            assertThat(cursor.next(), is(false));
            assertThat(cursor.next(), is(false));
        }
    }

    private static DataReader reader(int... values) {
        ByteBuffer buffer = Buffers.allocate((values.length * 2 + 1) * Integer.BYTES);
        for (int value : values) {
            buffer.putInt(Integer.BYTES);
            buffer.putInt(value);
        }
        buffer.putInt(-1);
        buffer.flip();
        return new ByteBufferReader(buffer);
    }

    private static void check(RecordCursor cursor, int expected) throws IOException, InterruptedException {
        assertThat(cursor.next(), is(true));
        ByteBuffer buf = cursor.get();
        int value = buf.getInt();
        assertThat(value, is(expected));
        assertThat(buf.hasRemaining(), is(false));
    }
}
