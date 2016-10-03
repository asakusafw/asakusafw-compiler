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
package com.asakusafw.dag.runtime.table;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.Test;

import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.runtime.value.IntOption;

/**
 * Test for {@link BasicDataTable}.
 */
public class BasicDataTableTest {

    /**
     * empty table.
     * @throws Exception if failed
     */
    @Test
    public void empty() throws Exception {
        BasicDataTable.Builder<IntOption> builder = start();

        DataTable<IntOption> table = builder.build();
        assertThat(sort(table.getList(key(100))), is(values()));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        BasicDataTable.Builder<IntOption> builder = start();
        builder.add(key(100), new IntOption(100));

        DataTable<IntOption> table = builder.build();
        assertThat(sort(table.getList(key(100))), is(values(100)));
        assertThat(sort(table.getList(key(101))), is(values()));
    }

    /**
     * conflict keys.
     * @throws Exception if failed
     */
    @Test
    public void duplicate() throws Exception {
        BasicDataTable.Builder<IntOption> builder = start();
        builder.add(key(100), new IntOption(100));
        builder.add(key(100), new IntOption(101));
        builder.add(key(100), new IntOption(102));

        DataTable<IntOption> table = builder.build();
        assertThat(sort(table.getList(key(100))), is(values(100, 101, 102)));
        assertThat(sort(table.getList(key(101))), is(values()));
    }

    /**
     * reuses the same key object.
     * @throws Exception if failed
     */
    @Test
    public void reuseKeys() throws Exception {
        BasicDataTable.Builder<IntOption> builder = start();
        KeyBuffer key = key();

        key.append(new IntOption(100));
        builder.add(key, new IntOption(100));
        key.clear();

        key.append(new IntOption(101));
        builder.add(key, new IntOption(101));
        key.clear();

        key.append(new IntOption(102));
        builder.add(key, new IntOption(102));
        key.clear();

        DataTable<IntOption> table = builder.build();
        assertThat(sort(table.getList(key(100))), is(values(100)));
        assertThat(sort(table.getList(key(101))), is(values(101)));
        assertThat(sort(table.getList(key(102))), is(values(102)));
    }

    private BasicDataTable.Builder<IntOption> start() {
        return new BasicDataTable.Builder<>(new LinkedHashMap<>(), HeapKeyBuffer::new);
    }

    private KeyBuffer key(int... values) {
        HeapKeyBuffer result = new HeapKeyBuffer();
        for (int value : values) {
            result.append(new IntOption(value));
        }
        return result;
    }

    private List<IntOption> sort(List<IntOption> list) {
        Collections.sort(list);
        return list;
    }

    private List<IntOption> values(int...values) {
        List<IntOption> options = new ArrayList<>();
        for (int value : values) {
            options.add(new IntOption(value));
        }
        return sort(options);
    }
}
