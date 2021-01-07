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
package com.asakusafw.dag.runtime.data;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;
import java.util.RandomAccess;

import org.junit.Test;

import com.asakusafw.runtime.value.IntOption;

/**
 * Test for {@link HeapListBuilder}.
 */
public class HeapListBuilderTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (HeapListBuilder<IntOption> builder = new HeapListBuilder<>(new IntOptionAdapter())) {
            int begin = 0;
            int end = 1;
            int size = end - begin;
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list, instanceOf(RandomAccess.class));
            assertThat(list.size(), is(size));
            for (int i = 0, n = end - begin; i < n; i++) {
                IntOption value = list.get(i);
                assertThat(value, is(new IntOption(i + begin)));
            }
        }
    }

    /**
     * w/ multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        try (HeapListBuilder<IntOption> builder = new HeapListBuilder<>(new IntOptionAdapter())) {
            int begin = 0;
            int end = 10;
            int size = end - begin;
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            for (int i = 0, n = end - begin; i < n; i++) {
                IntOption value = list.get(i);
                assertThat(value, is(new IntOption(i + begin)));
            }
        }
    }

    /**
     * w/ multiple records.
     * @throws Exception if failed
     */
    @Test
    public void large() throws Exception {
        try (HeapListBuilder<IntOption> builder = new HeapListBuilder<>(new IntOptionAdapter())) {
            int begin = 0;
            int end = 1_000_000;
            int size = end - begin;
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            for (int i = 0, n = end - begin; i < n; i++) {
                IntOption value = list.get(i);
                assertEquals(i + begin, value.get());
            }
        }
    }

    /**
     * w/ multiple records.
     * @throws Exception if failed
     */
    @Test
    public void reuse() throws Exception {
        try (HeapListBuilder<IntOption> builder = new HeapListBuilder<>(new IntOptionAdapter())) {
            int begin = 0;
            int end = 1_000_000;
            int size = end - begin;
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            for (int i = 0, n = end - begin; i < n; i++) {
                IntOption value = list.get(i);
                assertEquals(i + begin, value.get());
            }
        }
        try (HeapListBuilder<IntOption> builder = new HeapListBuilder<>(new IntOptionAdapter())) {
            int begin = 2_000_000;
            int end = 3_000_000;
            int size = end - begin;
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            for (int i = 0, n = end - begin; i < n; i++) {
                IntOption value = list.get(i);
                assertEquals(i + begin, value.get());
            }
        }
    }

    /**
     * w/ for-each.
     * @throws Exception if failed
     */
    @Test
    public void forEach() throws Exception {
        try (HeapListBuilder<IntOption> builder = new HeapListBuilder<>(new IntOptionAdapter())) {
            int begin = 0;
            int end = 10;
            int size = end - begin;
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            int index = begin;
            for (IntOption value : list) {
                assertThat(value, is(new IntOption(index++)));
            }
        }
    }

    /**
     * w/ for-each.
     * @throws Exception if failed
     */
    @Test
    public void forEach_replay() throws Exception {
        try (HeapListBuilder<IntOption> builder = new HeapListBuilder<>(new IntOptionAdapter())) {
            int begin = 0;
            int end = 10;
            int size = end - begin;
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            int index = begin;
            for (IntOption value : list) {
                assertThat(value, is(new IntOption(index++)));
            }
            index = begin;
            for (IntOption value : list) {
                assertThat(value, is(new IntOption(index++)));
            }
            index = begin;
            for (IntOption value : list) {
                assertThat(value, is(new IntOption(index++)));
            }
        }
    }

    /**
     * w/ out of lower bounds.
     * @throws Exception if failed
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void bounds_lower() throws Exception {
        try (HeapListBuilder<IntOption> builder = new HeapListBuilder<>(new IntOptionAdapter())) {
            builder.build(IntOptionAdapter.range(0, 10)).get(-1);
        }
    }

    /**
     * w/ out of upper bounds.
     * @throws Exception if failed
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void bounds_upper() throws Exception {
        try (HeapListBuilder<IntOption> builder = new HeapListBuilder<>(new IntOptionAdapter())) {
            builder.build(IntOptionAdapter.range(0, 10)).get(10);
        }
    }
}
