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
package com.asakusafw.dag.runtime.data;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.dag.runtime.data.SpillListBuilder.Options;

/**
 * Test for {@link SpillListBuilder}.
 */
public class SpillListBuilderTest {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(new IntOptionAdapter())) {
            int begin = 0;
            int end = 1;
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
    public void multiple() throws Exception {
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(new IntOptionAdapter())) {
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
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(new IntOptionAdapter())) {
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
    public void huge() throws Exception {
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(new IntOptionAdapter())) {
            int begin = 0;
            int end = 100_000_000;
            int size = end - begin;
            long t0 = System.currentTimeMillis();
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            long t1 = System.currentTimeMillis();
            for (int i = 0, n = end - begin; i < n; i++) {
                IntOption value = list.get(i);
                assertEquals(i + begin, value.get());
            }
            long t2 = System.currentTimeMillis();
            System.out.printf("spill - write: %,dms, read: %,dms%n", t1 - t0, t2 - t1);
        }
    }

    /**
     * w/ multiple records.
     * @throws Exception if failed
     */
    @Test
    public void reuse() throws Exception {
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(new IntOptionAdapter())) {
            int begin = 0;
            int end = 1_000_000;
            int size = end - begin;
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            for (int i = 0, n = end - begin; i < n; i++) {
                IntOption value = list.get(i);
                assertEquals(i + begin, value.get());
            }

            begin = 2_000_000;
            end = 3_000_000;
            size = end - begin;
            list = builder.build(IntOptionAdapter.range(begin, end));
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
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(new IntOptionAdapter())) {
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
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(new IntOptionAdapter())) {
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
     * w/ fragments.
     * @throws Exception if failed
     */
    @Test
    public void fragments() throws Exception {
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(
                new IntOptionAdapter(), 1024, 4096)) {
            int begin = 0;
            int end = 100_000;
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
     * just fragment size.
     * @throws Exception if failed
     */
    @Test
    public void fragments_just() throws Exception {
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(
                new IntOptionAdapter(), 1024, 5 * 1024 * 2)) {
            int begin = 0;
            int end = 100_000;
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
     * w/ fragments.
     * @throws Exception if failed
     */
    @Test
    public void fragments_reuse() throws Exception {
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(
                new IntOptionAdapter(), 2000, 4096)) {
            int begin = 0;
            int end = 100_000;
            int size = end - begin;
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            for (int i = 0, n = end - begin; i < n; i++) {
                IntOption value = list.get(i);
                assertEquals(i + begin, value.get());
            }

            begin = 100_000;
            end = 200_000;
            size = end - begin;
            list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            for (int i = 0, n = end - begin; i < n; i++) {
                IntOption value = list.get(i);
                assertEquals(i + begin, value.get());
            }
        }
    }

    /**
     * w/ fragments.
     * @throws Exception if failed
     */
    @Test
    public void fragments_huge() throws Exception {
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(
                new IntOptionAdapter(), 2000, 4096)) {
            int begin = 0;
            int end = 100_000_000;
            int size = end - begin;
            long t0 = System.currentTimeMillis();
            List<IntOption> list = builder.build(IntOptionAdapter.range(begin, end));
            assertThat(list.size(), is(size));
            long t1 = System.currentTimeMillis();
            for (int i = 0, n = end - begin; i < n; i++) {
                IntOption value = list.get(i);
                assertEquals(i + begin, value.get());
            }
            long t2 = System.currentTimeMillis();
            System.out.printf("fragments - write: %,dms, read: %,dms%n", t1 - t0, t2 - t1);
        }
    }

    /**
     * w/ directory.
     * @throws Exception if failed
     */
    @Test
    public void custom_directory() throws Exception {
        File dir = temporary.newFolder();
        dir.delete();
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(new IntOptionAdapter(), new Options()
                .withWindowSize(4)
                .withDirectory(dir.toPath()))) {
            builder.build(IntOptionAdapter.range(0, 5));
            assertThat(dir.isDirectory(), is(true));
        }
    }

    /**
     * w/ out of lower bounds.
     * @throws Exception if failed
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void bounds_lower() throws Exception {
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(new IntOptionAdapter())) {
            builder.build(IntOptionAdapter.range(0, 10)).get(-1);
        }
    }

    /**
     * w/ out of upper bounds.
     * @throws Exception if failed
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void bounds_upper() throws Exception {
        try (SpillListBuilder<IntOption> builder = new SpillListBuilder<>(new IntOptionAdapter())) {
            builder.build(IntOptionAdapter.range(0, 10)).get(10);
        }
    }
}
