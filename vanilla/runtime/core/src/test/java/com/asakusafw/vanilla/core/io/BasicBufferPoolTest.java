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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.vanilla.core.io.BufferPool.Ticket;
import com.asakusafw.vanilla.core.util.Buffers;

/**
 * Test for {@link BasicBufferPool}.
 */
public class BasicBufferPoolTest {

    /**
     * A temporary folder.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        BasicBufferPool pool = new BasicBufferPool(0, VoidStore.INSTANCE);
        assertThat(pool.getSize(), is(0L));
        try (Ticket ticket = pool.reserve(100)) {
            assertThat(ticket.getSize(), is(100L));
            assertThat(pool.getSize(), is(100L));
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * reserve - multiple tickets.
     * @throws Exception if failed
     */
    @Test
    public void reserve_multiple() throws Exception {
        BasicBufferPool pool = new BasicBufferPool(0, VoidStore.INSTANCE);
        assertThat(pool.getSize(), is(0L));
        try (Ticket t0 = pool.reserve(1);
                Ticket t1 = pool.reserve(2);
                Ticket t2 = pool.reserve(4)) {
            assertThat(t0.getSize(), is(1L));
            assertThat(t1.getSize(), is(2L));
            assertThat(t2.getSize(), is(4L));
            assertThat(pool.getSize(), is(7L));

            t2.close();
            assertThat(t0.getSize(), is(1L));
            assertThat(t1.getSize(), is(2L));
            assertThat(t2.getSize(), is(0L));
            assertThat(pool.getSize(), is(3L));

            t0.close();
            assertThat(t0.getSize(), is(0L));
            assertThat(t1.getSize(), is(2L));
            assertThat(t2.getSize(), is(0L));
            assertThat(pool.getSize(), is(2L));
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * register - simple.
     * @throws Exception if failed
     */
    @Test
    public void register() throws Exception {
        BasicBufferPool pool = new BasicBufferPool(100, VoidStore.INSTANCE);
        assertThat(pool.getSize(), is(0L));
        ByteBuffer buffer = buffer("Hello, world!");
        try (DataReader.Provider e0 = pool.register(pool.reserve(100), buffer)) {
            try (DataReader c = e0.open()) {
                assertThat(c.getBuffer(), is(notNullValue()));
            }
            assertThat(read(e0), is("Hello, world!"));
            assertThat(pool.getSize(), is((long) buffer.capacity()));
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * register - swap out.
     * @throws Exception if failed
     */
    @Test
    public void register_swapout() throws Exception {
        ByteBuffer buffer = buffer("Hello, world!");
        BasicBufferPool pool = new BasicBufferPool(buffer.capacity(), VoidStore.INSTANCE);
        assertThat(pool.getSize(), is(0L));
        try (DataReader.Provider e0 = pool.register(pool.reserve(buffer.capacity()), buffer)) {
            assertThat(pool.getSize(), is((long) buffer.capacity()));
            try (Ticket t1 = pool.reserve(1)) {
                Lang.pass(t1);
            }
            assertThat(read(e0), is("Hello, world!"));
            assertThat(pool.getSize(), is(0L));
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * register - swap out after once acquired.
     * @throws Exception if failed
     */
    @Test
    public void register_swapout_acquired() throws Exception {
        ByteBuffer buffer = buffer("Hello, world!");
        BasicBufferPool pool = new BasicBufferPool(buffer.capacity(), VoidStore.INSTANCE);
        assertThat(pool.getSize(), is(0L));
        try (DataReader.Provider e0 = pool.register(pool.reserve(buffer.capacity()), buffer)) {
            try (DataReader r0 = e0.open()) {
                assertThat(read(r0), is("Hello, world!"));

                // should swapout but e0 is currently acquired
                try (Ticket t1 = pool.reserve(1)) {
                    Lang.pass(t1);
                }
                assertThat(pool.getSize(), is((long) buffer.capacity()));
            }

            // should swapout -> ok
            try (Ticket t2 = pool.reserve(1)) {
                Lang.pass(t2);
            }
            assertThat(read(e0), is("Hello, world!"));
            assertThat(pool.getSize(), is(0L));
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * register - swap out.
     * @throws Exception if failed
     */
    @Test
    public void register_swapout_priority() throws Exception {
        ByteBuffer buffer = buffer("Hello, world!");
        BasicBufferPool pool = new BasicBufferPool(buffer.capacity() * 3, VoidStore.INSTANCE);
        assertThat(pool.getSize(), is(0L));
        try (DataReader.Provider e0 = pool.register(pool.reserve(buffer.capacity()), Buffers.duplicate(buffer), 3);
                DataReader.Provider e1 = pool.register(pool.reserve(buffer.capacity()), Buffers.duplicate(buffer), 1);
                DataReader.Provider e2 = pool.register(pool.reserve(buffer.capacity()), Buffers.duplicate(buffer), 2)) {
            assertThat(VoidStore.isAlive(e0), is(true));
            assertThat(VoidStore.isAlive(e1), is(true));
            assertThat(VoidStore.isAlive(e2), is(true));
            try (Ticket t1 = pool.reserve(1)) {
                Lang.pass(t1);
            }
            assertThat(VoidStore.isAlive(e0), is(true));
            assertThat(VoidStore.isAlive(e1), is(false));
            assertThat(VoidStore.isAlive(e2), is(true));
            try (Ticket t2 = pool.reserve(buffer.capacity() + 1)) {
                Lang.pass(t2);
            }
            assertThat(VoidStore.isAlive(e0), is(true));
            assertThat(VoidStore.isAlive(e1), is(false));
            assertThat(VoidStore.isAlive(e2), is(false));
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * register - move.
     * @throws Exception if failed
     */
    @Test
    public void register_move() throws Exception {
        BasicBufferPool pool = new BasicBufferPool(150, VoidStore.INSTANCE);
        assertThat(pool.getSize(), is(0L));
        ByteBuffer buffer = buffer("Hello, world!");
        try (DataReader.Provider e0 = pool.register(pool.reserve(100), buffer);
                BufferPool.Ticket t1 = pool.reserve(50)) {
            try (Ticket t2 = t1.move()) {
                assertThat(pool.getSize(), is(50L + buffer.capacity()));
            }
            assertThat(pool.getSize(), is(0L + buffer.capacity()));
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * register - shrink buffer.
     * @throws Exception if failed
     */
    @Test
    public void register_shrink() throws Exception {
        ByteBuffer buffer = Buffers.allocate(1_000_000);
        buffer.putInt(100);
        buffer.flip();

        BasicBufferPool pool = new BasicBufferPool(buffer.capacity(), VoidStore.INSTANCE);
        try (DataReader.Provider e0 = pool.register(pool.reserve(buffer.capacity()), buffer)) {
            assertThat(pool.getSize(), is(0L + Integer.BYTES));
        }
        assertThat(pool.getSize(), is(0L));
    }

    private static class VoidStore implements BufferStore {
        static final VoidStore INSTANCE = new VoidStore();

        private static final String MARK = "STORED";

        static boolean isAlive(DataReader.Provider entry) throws IOException, InterruptedException {
            try (DataReader reader = entry.open()) {
                return reader.toString().equals(MARK) == false;
            }
        }

        @Override
        public DataReader.Provider store(ByteBuffer buffer) throws IOException, InterruptedException {
            ByteBuffer capture = Buffers.duplicate(buffer);
            return new DataReader.Provider() {
                @Override
                public DataReader open() throws IOException, InterruptedException {
                    return new ByteBufferReader(capture) {
                        @Override
                        public String toString() {
                            return MARK;
                        }
                    };
                }
                @Override
                public void close() throws IOException, InterruptedException {
                    return;
                }
            };
        }
    }
}
