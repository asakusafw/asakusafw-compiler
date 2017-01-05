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
package com.asakusafw.dag.compiler.codegen;
import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import com.asakusafw.dag.api.processor.EdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.testing.CollectionGroupReader;
import com.asakusafw.dag.api.processor.testing.MockTaskProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.compiler.codegen.CoGroupInputAdapterGenerator.Spec;
import com.asakusafw.dag.runtime.adapter.CoGroupOperation;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.InputHandler.InputSession;
import com.asakusafw.dag.runtime.skeleton.CoGroupInputAdapter;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Lang;

/**
 * Test for {@link CoGroupInputAdapter}.
 */
public class CoGroupInputAdapterGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Map<String, SortedMap<String, List<MockDataModel>>> in = new LinkedHashMap<>();
        in.put("o0", Lang.let(new TreeMap<>(), m -> {
            m.put("A", ls(new MockDataModel("Hello, world!")));
        }));

        List<List<List<String>>> results = check(in);
        assertThat(results, is(ls(ls(ls("Hello, world!")))));
    }

    /**
     * multiple groups.
     */
    @Test
    public void co() {
        Map<String, SortedMap<String, List<MockDataModel>>> in = new LinkedHashMap<>();
        in.put("o0", Lang.let(new TreeMap<>(), m -> {
            m.put("A", ls(new MockDataModel("A-0-0")));
        }));
        in.put("o1", Lang.let(new TreeMap<>(), m -> {
            m.put("A", ls(new MockDataModel("A-1-0")));
        }));
        in.put("o2", Lang.let(new TreeMap<>(), m -> {
            m.put("A", ls(new MockDataModel("A-2-0")));
        }));

        List<List<List<String>>> results = check(in);
        assertThat(results, is(ls(ls(ls("A-0-0"), ls("A-1-0"), ls("A-2-0")))));
    }

    /**
     * multiple entries.
     */
    @Test
    public void entries() {
        Map<String, SortedMap<String, List<MockDataModel>>> in = new LinkedHashMap<>();
        in.put("o0", Lang.let(new TreeMap<>(), m -> {
            m.put("A", ls(new MockDataModel("A-0-0"), new MockDataModel("A-0-1"), new MockDataModel("A-0-2")));
        }));
        in.put("o1", Lang.let(new TreeMap<>(), m -> {
            m.put("A", ls(new MockDataModel("A-1-0"), new MockDataModel("A-1-1"), new MockDataModel("A-1-2")));
        }));

        List<List<List<String>>> results = check(in);
        assertThat(results, is(ls(ls(ls("A-0-0", "A-0-1", "A-0-2"), ls("A-1-0", "A-1-1", "A-1-2")))));
    }

    /**
     * multiple groups.
     */
    @Test
    public void groups() {
        Map<String, SortedMap<String, List<MockDataModel>>> in = new LinkedHashMap<>();
        in.put("o0", Lang.let(new TreeMap<>(), m -> {
            m.put("A", ls(new MockDataModel("A-0-0")));
            m.put("B", ls(new MockDataModel("B-0-1"), new MockDataModel("B-0-2")));
        }));
        in.put("o1", Lang.let(new TreeMap<>(), m -> {
            m.put("A", ls(new MockDataModel("A-1-0")));
            m.put("C", ls(new MockDataModel("C-1-1"), new MockDataModel("C-1-2")));
        }));

        List<List<List<String>>> results = check(in);
        assertThat(results, is(ls(ls(ls("A-0-0"), ls("A-1-0")),
                ls(ls("B-0-1", "B-0-2"), ls()),
                ls(ls(), ls("C-1-1", "C-1-2")))));
    }

    @SafeVarargs
    private static <T> List<T> ls(T... values) {
        return Arrays.asList(values);
    }

    private List<List<List<String>>> check(Map<String, SortedMap<String, List<MockDataModel>>> map) {
        ClassGeneratorContext gc = context();
        ClassDescription generated = add(c -> new CoGroupInputAdapterGenerator().generate(
                gc, Lang.project(map.keySet(), s -> new Spec(
                        s, typeOf(MockDataModel.class), CoGroupInputAdapter.BufferType.HEAP)), c));

        MockTaskProcessorContext tc = new MockTaskProcessorContext("t");
        map.forEach((in, v) -> tc.withInput(in, () -> new CollectionGroupReader(v)));

        List<List<List<String>>> results = new ArrayList<>();
        loading(generated, c -> {
            try (CoGroupInputAdapter adapter = adapter(c, new MockVertexProcessorContext().with(c))) {
                adapter.initialize();

                InputHandler<CoGroupOperation.Input, ? super EdgeIoProcessorContext> handler = adapter.newHandler();
                try (InputSession<CoGroupOperation.Input> session = handler.start(tc)) {
                    while (session.next()) {
                        CoGroupOperation.Input input = session.get();
                        List<List<String>> g = new ArrayList<>();
                        for (int i = 0, n = map.size(); i < n; i++) {
                            g.add(Lang.project(
                                    input.<MockDataModel>getList(i),
                                    m -> m.getValueOption().getAsString()));
                        }
                        results.add(g);
                    }
                }
            }
        });
        return results;
    }
}
