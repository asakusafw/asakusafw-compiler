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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.asakusafw.dag.api.common.TaggedSupplier;
import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.compiler.codegen.UnionRecordSerDeSupplierGenerator.Downstream;
import com.asakusafw.dag.compiler.codegen.UnionRecordSerDeSupplierGenerator.Upstream;
import com.asakusafw.dag.runtime.io.UnionRecord;
import com.asakusafw.dag.runtime.io.UnionRecordSerDeSupplier;
import com.asakusafw.dag.runtime.testing.IntSerDe;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.runtime.io.util.DataBuffer;

/**
 * Test for {@link UnionRecordSerDeSupplierGenerator}.
 */
public class UnionRecordSerDeSupplierGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * ser/de - simple case.
     * @throws Exception if failed
     */
    @Test
    public void serde() throws Exception {
        run(Arrays.asList(Upstream.of(classOf(IntSerDe.class), "a")), Downstream.of(null), supplier -> {
            DataBuffer buffer = new DataBuffer();
            supplier.get("a").serialize(100, buffer);
            ValueSerDe deser = supplier.get();
            assertThat(deser.deserialize(buffer), is(new UnionRecord(0, 100)));
        });
    }

    /**
     * ser/de - multiple segments.
     * @throws Exception if failed
     */
    @Test
    public void serde_multiple() throws Exception {
        run(Arrays.asList(
                Upstream.of(classOf(E1.class), "a"),
                Upstream.of(classOf(E2.class), "b"),
                Upstream.of(classOf(E3.class), "c")), Downstream.of(null), supplier -> {
            DataBuffer buffer = new DataBuffer();
            supplier.get("a").serialize(100, buffer);
            supplier.get("b").serialize(200, buffer);
            supplier.get("c").serialize(300, buffer);
            ValueSerDe deser = supplier.get();
            assertThat(deser.deserialize(buffer), is(new UnionRecord(0, 101)));
            assertThat(deser.deserialize(buffer), is(new UnionRecord(1, 202)));
            assertThat(deser.deserialize(buffer), is(new UnionRecord(2, 303)));
        });
    }

    /**
     * ser/de - cascade.
     * @throws Exception if failed
     */
    @Test
    public void serde_cascade() throws Exception {
        run(Arrays.asList(
                Upstream.of(classOf(E1.class), "a", "b"),
                Upstream.of(classOf(E2.class), "b", "c")), Downstream.of(null), supplier -> {
            DataBuffer buffer = new DataBuffer();
            supplier.get("a").serialize(100, buffer);
            supplier.get("b").serialize(200, buffer);
            supplier.get("c").serialize(300, buffer);
            ValueSerDe deser = supplier.get();
            assertThat(deser.deserialize(buffer), is(new UnionRecord(0, 101)));
            assertThat(deser.deserialize(buffer), is(new UnionRecord(0, 201, new UnionRecord(1, 202))));
            assertThat(deser.deserialize(buffer), is(new UnionRecord(1, 302)));
        });
    }

    private void run(List<Upstream> ups, Downstream down, Action<TaggedSupplier<ValueSerDe>, ?> action) {
        loading(add(UnionRecordSerDeSupplierGenerator.generate(context(), ups, down)), c -> {
            UnionRecordSerDeSupplier supplier = (UnionRecordSerDeSupplier) c.newInstance();
            action.perform(supplier);
        });
    }

    @SuppressWarnings("javadoc")
    public static class E1 extends IntSerDe {
        public E1() {
            super(1, 0);
        }
    }

    @SuppressWarnings("javadoc")
    public static class E2 extends IntSerDe {
        public E2() {
            super(2, 0);
        }
    }

    @SuppressWarnings("javadoc")
    public static class E3 extends IntSerDe {
        public E3() {
            super(3, 0);
        }
    }
}
