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
package com.asakusafw.dag.runtime.table;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Test;

import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.table.BasicDataTable.KeyValidator;
import com.asakusafw.dag.runtime.table.BasicDataTable.ValidationLevel;
import com.asakusafw.lang.utils.common.AssertUtil;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;

/**
 * Test for {@link BasicDataTable}.
 */
public class BasicDataTableTest {

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
     * empty elements.
     * @throws Exception if failed
     */
    @Test
    public void empty_elements() throws Exception {
        BasicDataTable.Builder<IntOption> builder = start();

        DataTable<IntOption> table = builder.build();
        assertThat(sort(table.getList(key(100))), is(values()));
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
     * sorted elements.
     * @throws Exception if failed
     */
    @Test
    public void sorted() throws Exception {
        BasicDataTable.Builder<IntOption> builder = start();
        builder.add(key(100), new IntOption(101));
        builder.add(key(100), new IntOption(102));
        builder.add(key(100), new IntOption(100));
        builder.add(key(100), new IntOption(104));
        builder.add(key(100), new IntOption(103));

        DataTable<IntOption> table = builder.build(Comparator.naturalOrder());
        assertThat(table.getList(key(100)), is(values(100, 101, 102, 103, 104)));
    }

    /**
     * reuses the same key object.
     * @throws Exception if failed
     */
    @Test
    public void reuseKeys() throws Exception {
        BasicDataTable.Builder<IntOption> builder = start();
        KeyBuffer key = builder.newKeyBuffer();

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

    /**
     * as iterable.
     * @throws Exception if failed
     */
    @Test
    public void iterable() throws Exception {
        BasicDataTable.Builder<Integer> builder = start();
        builder.add(key(1), 1);
        builder.add(key(1), 2);
        builder.add(key(2), 3);

        DataTable<Integer> table = builder.build();
        List<Integer> list = new ArrayList<>();
        for (int v : table) {
            list.add(v);
        }
        assertThat(sort(list), contains(1, 2, 3));
    }

    /**
     * empty table.
     * @throws Exception if failed
     */
    @Test
    public void empty_table() throws Exception {
        for (Object o : BasicDataTable.empty()) {
            fail(String.valueOf(o));
        }
    }

    /**
     * find w/ 0-parameters.
     * @throws Exception if failed
     */
    @Test
    public void find0() throws Exception {
        BasicDataTable.Builder<Integer> builder = start();
        builder.add(key(), 0);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(), contains(0));
    }

    /**
     * find w/ 1-parameter.
     * @throws Exception if failed
     */
    @Test
    public void find1() throws Exception {
        BasicDataTable.Builder<Integer> builder = start();
        builder.add(key(0), 1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0)), contains(1));
        assertThat(table.find(k(1)), hasSize(0));
    }

    /**
     * find w/ 2-parametes.
     * @throws Exception if failed
     */
    @Test
    public void find2() throws Exception {
        BasicDataTable.Builder<Integer> builder = start();
        builder.add(key(0, 1), 2);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1)), contains(2));
        assertThat(table.find(k(1), k(2)), hasSize(0));
    }

    /**
     * find w/ 3-parametes.
     * @throws Exception if failed
     */
    @Test
    public void find3() throws Exception {
        BasicDataTable.Builder<Integer> builder = start();
        builder.add(key(0, 1, 2), 3);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1), k(2)), contains(3));
        assertThat(table.find(k(1), k(2), k(3)), hasSize(0));
    }

    /**
     * find w/ 4-parametes.
     * @throws Exception if failed
     */
    @Test
    public void find4() throws Exception {
        BasicDataTable.Builder<Integer> builder = start();
        builder.add(key(0, 1, 2, 3), 4);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1), k(2), k(3)), contains(4));
        assertThat(table.find(k(1), k(2), k(3), k(4)), hasSize(0));
    }

    /**
     * find w/ N-parametes.
     * @throws Exception if failed
     */
    @Test
    public void findN() throws Exception {
        BasicDataTable.Builder<Integer> builder = start();
        builder.add(key(0, 1, 2, 3, 4, 5), 6);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1), k(2), k(3), k(4), k(5)), contains(6));
        assertThat(table.find(k(1), k(2), k(3), k(4), k(5), k(6)), hasSize(0));
    }

    /**
     * validates nothing.
     * @throws Exception if failed
     */
    @Test
    public void validate_nothing() throws Exception {
        BasicDataTable.Builder<Integer> builder = start();
        builder.add(key(0, 1), 2);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(), hasSize(0));
        assertThat(table.find(k(0)), hasSize(0));
        assertThat(table.find(k(0), k(1)), contains(2));
        assertThat(table.find(k(0), k(2)), hasSize(0));
        assertThat(table.find(k(0), k(1), k(2)), hasSize(0));
        assertThat(table.find(k(0), k(1), k(2), k(3)), hasSize(0));
        assertThat(table.find(k(0), k(1), k(2), k(3), k(4)), hasSize(0));
    }

    /**
     * validates element count.
     * @throws Exception if failed
     */
    @Test
    public void validate_count0() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(0);
        builder.add(key(), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(), hasSize(1));

        AssertUtil.catching(() -> table.find(k(0)));
        AssertUtil.catching(() -> table.find(k(0), k(1)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3), k(4)));
    }

    /**
     * validates element count.
     * @throws Exception if failed
     */
    @Test
    public void validate_count1() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(1);
        builder.add(key(0), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0)), hasSize(1));
        assertThat(table.find(new LongOption(0)), hasSize(0));

        AssertUtil.catching(() -> table.find());
        AssertUtil.catching(() -> table.find(k(0), k(1)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3), k(4)));
    }

    /**
     * validates element count.
     * @throws Exception if failed
     */
    @Test
    public void validate_count2() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(2);
        builder.add(key(0, 1), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1)), hasSize(1));
        assertThat(table.find(k(0), new LongOption(1)), hasSize(0));

        AssertUtil.catching(() -> table.find());
        AssertUtil.catching(() -> table.find(k(0)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3), k(4)));
    }

    /**
     * validates element count.
     * @throws Exception if failed
     */
    @Test
    public void validate_count3() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(3);
        builder.add(key(0, 1, 2), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1), k(2)), hasSize(1));
        assertThat(table.find(k(0), k(1), new LongOption(2)), hasSize(0));

        AssertUtil.catching(() -> table.find());
        AssertUtil.catching(() -> table.find(k(0)));
        AssertUtil.catching(() -> table.find(k(0), k(1)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3), k(4)));
    }

    /**
     * validates element count.
     * @throws Exception if failed
     */
    @Test
    public void validate_count4() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(4);
        builder.add(key(0, 1, 2, 3), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1), k(2), k(3)), hasSize(1));
        assertThat(table.find(k(0), k(1), k(2), new LongOption(3)), hasSize(0));

        AssertUtil.catching(() -> table.find());
        AssertUtil.catching(() -> table.find(k(0)));
        AssertUtil.catching(() -> table.find(k(0), k(1)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3), k(4)));
    }

    /**
     * validates element count.
     * @throws Exception if failed
     */
    @Test
    public void validate_countN() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(6);
        builder.add(key(0, 1, 2, 3, 4, 5), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1), k(2), k(3), k(4), k(5)), hasSize(1));
        assertThat(table.find(k(0), k(1), k(2), k(3), k(4), new LongOption(5)), hasSize(0));

        AssertUtil.catching(() -> table.find());
        AssertUtil.catching(() -> table.find(k(0)));
        AssertUtil.catching(() -> table.find(k(0), k(1)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3), k(4)));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3), k(4), k(5), k(6)));
    }

    /**
     * validates element types.
     * @throws Exception if failed
     */
    @Test
    public void validate_type1() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(classes(1));
        builder.add(key(0), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0)), hasSize(1));
        AssertUtil.catching(() -> table.find(0));
    }

    /**
     * validates element types.
     * @throws Exception if failed
     */
    @Test
    public void validate_type2() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(classes(2));
        builder.add(key(0, 1), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1)), hasSize(1));
        AssertUtil.catching(() -> table.find(k(0), 1));
    }

    /**
     * validates element types.
     * @throws Exception if failed
     */
    @Test
    public void validate_type3() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(classes(3));
        builder.add(key(0, 1, 2), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1), k(2)), hasSize(1));
        AssertUtil.catching(() -> table.find(k(0), k(1), 2));
    }

    /**
     * validates element types.
     * @throws Exception if failed
     */
    @Test
    public void validate_type4() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(classes(4));
        builder.add(key(0, 1, 2, 3), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1), k(2), k(3)), hasSize(1));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), 3));
    }

    /**
     * validates element types.
     * @throws Exception if failed
     */
    @Test
    public void validate_typeN() throws Exception {
        BasicDataTable.Builder<Integer> builder = start(classes(6));
        builder.add(key(0, 1, 2, 3, 4, 5), -1);
        DataTable<Integer> table = builder.build();

        assertThat(table.find(k(0), k(1), k(2), k(3), k(4), k(5)), hasSize(1));
        AssertUtil.catching(() -> table.find(k(0), k(1), k(2), k(3), k(4), 5));
    }

    private <T> BasicDataTable.Builder<T> start() {
        return new BasicDataTable.Builder<>(new LinkedHashMap<>(), HeapKeyBuffer::new);
    }

    private <T> BasicDataTable.Builder<T> start(int count) {
        return new BasicDataTable.Builder<>(
                new LinkedHashMap<>(), HeapKeyBuffer::new,
                new KeyValidator(ValidationLevel.COUNT, classes(count)));
    }

    private <T> BasicDataTable.Builder<T> start(Class<?>... types) {
        return new BasicDataTable.Builder<>(
                new LinkedHashMap<>(), HeapKeyBuffer::new,
                new KeyValidator(ValidationLevel.TYPE, types));
    }

    private Class<?>[] classes(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> IntOption.class)
                .toArray(Class[]::new);
    }

    private IntOption k(int value) {
        return new IntOption(value);
    }

    private KeyBuffer key(int... values) {
        HeapKeyBuffer result = new HeapKeyBuffer();
        for (int value : values) {
            result.append(new IntOption(value));
        }
        return result;
    }

    private <T extends Comparable<? super T>> List<T> sort(List<T> list) {
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
