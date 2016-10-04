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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.OutputNode;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.testing.MockResult;

/**
 * Test for {@link BufferOperatorGenerator}.
 */
public class BufferOperatorGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockResult<MockDataModel> m0 = new MockResult<>();
        MockResult<MockDataModel> m1 = new MockResult<>();
        check(Arrays.asList(m0, m1), r -> {
            MockDataModel o = new MockDataModel();
            o.setKey(1);
            r.add(o);
        });
        assertThat(m0.getResults(), hasSize(1));
        assertThat(m1.getResults(), hasSize(1));
        assertThat(m0.getResults().get(0).getKey(), is(1));
        assertThat(m1.getResults().get(0).getKey(), is(1));
    }

    /**
     * w/ edit sources.
     */
    @Test
    public void edit() {
        MockResult<MockDataModel> m0 = new MockResult<MockDataModel>() {
            @Override
            protected MockDataModel bless(MockDataModel result) {
                result.setKey(result.getKey() + 1);
                return new MockDataModel(result);
            }
        };
        MockResult<MockDataModel> m1 = new MockResult<MockDataModel>() {
            @Override
            protected MockDataModel bless(MockDataModel result) {
                result.setKey(result.getKey() + 2);
                return new MockDataModel(result);
            }
        };
        check(Arrays.asList(m0, m1), r -> {
            MockDataModel o = new MockDataModel();
            o.setKey(1);
            r.add(o);
        });
        assertThat(m0.getResults(), hasSize(1));
        assertThat(m1.getResults(), hasSize(1));
        assertThat(m0.getResults().get(0).getKey(), is(2));
        assertThat(m1.getResults().get(0).getKey(), is(3));
    }

    /**
     * multiple branches.
     */
    @Test
    public void multiple() {
        MockResult<MockDataModel> m0 = new MockResult<MockDataModel>() {
            @Override
            protected MockDataModel bless(MockDataModel result) {
                result.setKey(result.getKey() + 1);
                return new MockDataModel(result);
            }
        };
        MockResult<MockDataModel> m1 = new MockResult<MockDataModel>() {
            @Override
            protected MockDataModel bless(MockDataModel result) {
                result.setKey(result.getKey() + 2);
                return new MockDataModel(result);
            }
        };
        MockResult<MockDataModel> m2 = new MockResult<MockDataModel>() {
            @Override
            protected MockDataModel bless(MockDataModel result) {
                result.setKey(result.getKey() + 3);
                return new MockDataModel(result);
            }
        };
        MockResult<MockDataModel> m3 = new MockResult<MockDataModel>() {
            @Override
            protected MockDataModel bless(MockDataModel result) {
                result.setKey(result.getKey() + 4);
                return new MockDataModel(result);
            }
        };
        check(Arrays.asList(m0, m1, m2, m3), r -> {
            MockDataModel o = new MockDataModel();
            o.setKey(1);
            r.add(o);
        });
        assertThat(m0.getResults(), hasSize(1));
        assertThat(m1.getResults(), hasSize(1));
        assertThat(m2.getResults(), hasSize(1));
        assertThat(m3.getResults(), hasSize(1));
        assertThat(m0.getResults().get(0).getKey(), is(2));
        assertThat(m1.getResults().get(0).getKey(), is(3));
        assertThat(m2.getResults().get(0).getKey(), is(4));
        assertThat(m3.getResults().get(0).getKey(), is(5));
    }

    /**
     * cache - simple.
     */
    @Test
    public void cache() {
        ClassData a = BufferOperatorGenerator.generate(context(), outputs(typeOf(MockDataModel.class), 2));
        ClassData b = BufferOperatorGenerator.generate(context(), outputs(typeOf(MockDataModel.class), 2));
        assertThat(b, is(cacheOf(a)));
    }

    /**
     * cache w/ different types.
     */
    @Test
    public void cache_diff_type() {
        ClassData a = BufferOperatorGenerator.generate(context(), outputs(typeOf(MockDataModel.class), 2));
        ClassData b = BufferOperatorGenerator.generate(context(), outputs(typeOf(MockKeyValueModel.class), 2));
        assertThat(b, is(not(cacheOf(a))));
    }

    /**
     * cache w/ different counts.
     */
    @Test
    public void cache_diff_count() {
        ClassData a = BufferOperatorGenerator.generate(context(), outputs(typeOf(MockDataModel.class), 2));
        ClassData b = BufferOperatorGenerator.generate(context(), outputs(typeOf(MockDataModel.class), 3));
        assertThat(b, is(not(cacheOf(a))));
    }

    private void check(List<? extends Result<MockDataModel>> list, Consumer<Result<MockDataModel>> callback) {
        List<VertexElement> succs = new ArrayList<>();
        Class<?>[] parameterTypes = new Class<?>[list.size()];
        Object[] arguments = new Object[list.size()];
        for (int i = 0, n = list.size(); i < n; i++) {
            succs.add(new OutputNode("o" + i, typeOf(Result.class), typeOf(MockDataModel.class)));
            parameterTypes[i] = Result.class;
            arguments[i] = list.get(i);
        }
        ClassGeneratorContext context = context();
        ClassDescription generated = BufferOperatorGenerator.get(context, succs);
        loading(generated, c -> {
            Constructor<?> ctor = c.getConstructor(list.stream().map(r -> Result.class).toArray(Class[]::new));
            @SuppressWarnings("unchecked")
            Result<MockDataModel> r = (Result<MockDataModel>) ctor.newInstance(list.stream().toArray());
            callback.accept(r);
        });
    }

    private List<VertexElement> outputs(TypeDescription type, int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new OutputNode("o" + i, typeOf(Result.class), type))
                .collect(Collectors.toList());
    }
}
