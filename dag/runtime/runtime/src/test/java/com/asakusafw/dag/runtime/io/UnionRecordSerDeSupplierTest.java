/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.dag.runtime.io;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.runtime.testing.IntSerDe;
import com.asakusafw.runtime.io.util.DataBuffer;

/**
 * Test for {@link UnionRecordSerDeSupplier}.
 */
public class UnionRecordSerDeSupplierTest {

    /**
     * ser/de - simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        UnionRecordSerDeSupplier supplier = new UnionRecordSerDeSupplier()
            .upstream(Arrays.asList("a"), () -> new IntSerDe())
            .downstream(null);
        DataBuffer buffer = new DataBuffer();
        supplier.get("a").serialize(100, buffer);
        ValueSerDe deser = supplier.get();
        assertThat(deser.deserialize(buffer), is(new UnionRecord(0, 100)));
    }

    /**
     * ser/de - multiple segments.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        UnionRecordSerDeSupplier supplier = new UnionRecordSerDeSupplier()
                .upstream(Arrays.asList("a"), () -> new IntSerDe(1, 0))
                .upstream(Arrays.asList("b"), () -> new IntSerDe(2, 0))
                .upstream(Arrays.asList("c"), () -> new IntSerDe(3, 0))
                .downstream(null);
        DataBuffer buffer = new DataBuffer();
        supplier.get("a").serialize(100, buffer);
        supplier.get("b").serialize(200, buffer);
        supplier.get("c").serialize(300, buffer);
        ValueSerDe deser = supplier.get();
        assertThat(deser.deserialize(buffer), is(new UnionRecord(0, 101)));
        assertThat(deser.deserialize(buffer), is(new UnionRecord(1, 202)));
        assertThat(deser.deserialize(buffer), is(new UnionRecord(2, 303)));
    }

    /**
     * ser/de - cascade.
     * @throws Exception if failed
     */
    @Test
    public void cascade() throws Exception {
        UnionRecordSerDeSupplier supplier = new UnionRecordSerDeSupplier()
                .upstream(Arrays.asList("a", "b"), () -> new IntSerDe(1, 0))
                .upstream(Arrays.asList("b", "c"), () -> new IntSerDe(2, 0))
                .downstream(null);
        DataBuffer buffer = new DataBuffer();
        supplier.get("a").serialize(100, buffer);
        supplier.get("b").serialize(200, buffer);
        supplier.get("c").serialize(300, buffer);
        ValueSerDe deser = supplier.get();
        assertThat(deser.deserialize(buffer), is(new UnionRecord(0, 101)));
        assertThat(deser.deserialize(buffer), is(new UnionRecord(0, 201, new UnionRecord(1, 202))));
        assertThat(deser.deserialize(buffer), is(new UnionRecord(1, 302)));
    }
}
