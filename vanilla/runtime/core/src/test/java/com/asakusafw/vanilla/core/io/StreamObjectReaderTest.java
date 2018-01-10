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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.asakusafw.dag.runtime.testing.IntSerDe;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * Test for {@link StreamObjectReader}.
 */
public class StreamObjectReaderTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        int[][] inputs = {
                { 100 },
        };
        try (StreamObjectReader r = new StreamObjectReader(stream(inputs), new IntSerDe())) {
            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(100));

            assertThat(r.nextObject(), is(false));
        }
    }

    /**
     * w/o chunks.
     * @throws Exception if failed
     */
    @Test
    public void empty_chunks() throws Exception {
        int[][] inputs = {
        };
        try (StreamObjectReader r = new StreamObjectReader(stream(inputs), new IntSerDe())) {
            assertThat(r.nextObject(), is(false));
        }
    }

    /**
     * w/o records.
     * @throws Exception if failed
     */
    @Test
    public void empty_records() throws Exception {
        int[][] inputs = {
                { },
        };
        try (StreamObjectReader r = new StreamObjectReader(stream(inputs), new IntSerDe())) {
            assertThat(r.nextObject(), is(false));
        }
    }

    /**
     * w/ multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple_records() throws Exception {
        int[][] inputs = {
                { 100, 200, 300, },
        };
        try (StreamObjectReader r = new StreamObjectReader(stream(inputs), new IntSerDe())) {
            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(100));

            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(200));

            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(300));

            assertThat(r.nextObject(), is(false));
        }
    }

    /**
     * w/ multiple chunks.
     * @throws Exception if failed
     */
    @Test
    public void multiple_chunks() throws Exception {
        int[][] inputs = {
                { 100 },
                { },
                { 101 },
                { },
                { 102 },
        };
        try (StreamObjectReader r = new StreamObjectReader(stream(inputs), new IntSerDe())) {
            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(100));

            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(101));

            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(102));

            assertThat(r.nextObject(), is(false));
        }
    }

    private static RecordCursor.Stream stream(int[][] records){
        return new RecordCursor.Stream() {
            private int chunkIndex = 0;
            @Override
            public RecordCursor poll() throws IOException, InterruptedException {
                if (chunkIndex >= records.length) {
                    return null;
                }
                int[] currentChunk = records[chunkIndex++];
                return new RecordCursor() {
                    private int recordIndex = 0;
                    private ByteBuffer currentRecord;
                    @Override
                    public boolean next() throws IOException, InterruptedException {
                        if (recordIndex >= currentChunk.length) {
                            currentRecord = null;
                            return false;
                        }
                        if (currentRecord == null) {
                            currentRecord = Buffers.allocate(Integer.BYTES);
                        }
                        currentRecord.clear();
                        currentRecord.putInt(currentChunk[recordIndex++]);
                        currentRecord.flip();
                        return true;
                    }
                    @Override
                    public ByteBuffer get() throws IOException, InterruptedException {
                        return Invariants.requireNonNull(currentRecord);
                    }
                    @Override
                    public void close() throws IOException, InterruptedException {
                        return;
                    }
                };
            }
        };
    }
}
