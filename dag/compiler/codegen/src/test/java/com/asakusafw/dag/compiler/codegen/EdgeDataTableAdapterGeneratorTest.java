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
package com.asakusafw.dag.compiler.codegen;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.Test;

import com.asakusafw.dag.api.processor.testing.CollectionObjectReader;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.compiler.codegen.EdgeDataTableAdapterGenerator.Spec;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.DataTableAdapter;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Groups;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.value.IntOption;

/**
 * Test for {@link EdgeDataTableAdapterGenerator}.
 */
public class EdgeDataTableAdapterGeneratorTest extends ClassGeneratorTestRoot {

    private final List<Spec> specs = new ArrayList<>();

    private final Map<String, List<Object>> inputs = new LinkedHashMap<>();

    /**
     * simple case.
     */
    @Test
    public void simple() {
        define("t", "i", MockDataModel.class, "key");
        data("i", new Object[] {
                new MockDataModel(0, "Hello0"),
                new MockDataModel(1, "Hello1a"),
                new MockDataModel(1, "Hello1b"),
                new MockDataModel(2, "Hello2"),
        });
        check(a -> {
            DataTable<MockDataModel> t = a.getDataTable(MockDataModel.class, "t");
            assertThat(get(t, MockDataModel::getValue, 0), containsInAnyOrder("Hello0"));
            assertThat(get(t, MockDataModel::getValue, 1), containsInAnyOrder("Hello1a", "Hello1b"));
            assertThat(get(t, MockDataModel::getValue, 2), containsInAnyOrder("Hello2"));
            assertThat(get(t, MockDataModel::getValue, 3), hasSize(0));
        });
    }

    /**
     * w/ empty group.
     */
    @Test
    public void empty_group() {
        define("t", "i", MockDataModel.class);
        data("i", new Object[] {
                new MockDataModel(0, "Hello0"),
                new MockDataModel(1, "Hello1"),
                new MockDataModel(2, "Hello2"),
        });
        check(a -> {
            DataTable<MockDataModel> t = a.getDataTable(MockDataModel.class, "t");
            assertThat(get(t, MockDataModel::getValue), containsInAnyOrder("Hello0", "Hello1", "Hello2"));
        });
    }

    /**
     * w/ sorted.
     */
    @Test
    public void sorted() {
        define("t", "i", MockDataModel.class, "key", "+value");
        data("i", new Object[] {
                new MockDataModel(0, "0"),
                new MockDataModel(1, "1a"),
                new MockDataModel(1, "1c"),
                new MockDataModel(1, "1e"),
                new MockDataModel(1, "1d"),
                new MockDataModel(1, "1b"),
                new MockDataModel(2, "2c"),
                new MockDataModel(2, "2b"),
                new MockDataModel(2, "2a"),
        });
        check(a -> {
            DataTable<MockDataModel> t = a.getDataTable(MockDataModel.class, "t");
            assertThat(get(t, MockDataModel::getValue, 0), contains("0"));
            assertThat(get(t, MockDataModel::getValue, 1), contains("1a", "1b", "1c", "1d", "1e"));
            assertThat(get(t, MockDataModel::getValue, 2), contains("2a", "2b", "2c"));
            assertThat(get(t, MockDataModel::getValue, 3), hasSize(0));
        });
    }

    /**
     * multiple tables.
     */
    @Test
    public void multiple() {
        define("t0", "i0", MockDataModel.class, "key");
        define("t1", "i1", MockDataModel.class, "key");
        data("i0", new Object[] {
                new MockDataModel(0, "Hello0"),
        });
        data("i1", new Object[] {
                new MockDataModel(1, "Hello1"),
        });
        check(a -> {
            DataTable<MockDataModel> t0 = a.getDataTable(MockDataModel.class, "t0");
            assertThat(get(t0, MockDataModel::getValue, 0), containsInAnyOrder("Hello0"));
            assertThat(get(t0, MockDataModel::getValue, 1), hasSize(0));

            DataTable<MockDataModel> t1 = a.getDataTable(MockDataModel.class, "t1");
            assertThat(get(t1, MockDataModel::getValue, 0), hasSize(0));
            assertThat(get(t1, MockDataModel::getValue, 1), containsInAnyOrder("Hello1"));
        });
    }

    private void define(String tId, String iId, Class<?> type, String... terms) {
        specs.add(new Spec(tId, iId, Descriptions.typeOf(type), Groups.parse(terms)));
    }

    private void data(String iId, Object... values) {
        inputs.put(iId, Arrays.asList(values));
    }

    private void check(Action<DataTableAdapter, Exception> callback) {
        ClassGeneratorContext gc = context();
        ClassDescription gen = add(c -> new EdgeDataTableAdapterGenerator().generate(gc, specs, c));
        loading(gen, c -> {
            MockVertexProcessorContext context = new MockVertexProcessorContext().with(c);
            inputs.forEach((k, v) -> {
                context.withInput(k, () -> new CollectionObjectReader(v));
            });
            try (DataTableAdapter adapter = adapter(c, context)) {
                adapter.initialize();
                callback.perform(adapter);
            }
        });
    }

    private <T, U> List<U> get(DataTable<T> table, Function<T, U> mapper, int... key) {
        List<T> list = table.getList(key(table, key));
        return Lang.project(list, mapper);
    }

    private KeyBuffer key(DataTable<?> table, int... values) {
        KeyBuffer key = table.newKeyBuffer();
        for (int value : values) {
            key.append(new IntOption(value));
        }
        return key;
    }
}
