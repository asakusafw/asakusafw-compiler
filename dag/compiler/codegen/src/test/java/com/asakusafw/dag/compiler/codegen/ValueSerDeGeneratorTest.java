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

import java.math.BigDecimal;

import org.junit.Test;

import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.io.util.DataBuffer;

/**
 * Test for {@link ValueSerDeGenerator}.
 */
@SuppressWarnings("deprecation")
public class ValueSerDeGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ClassDescription gen = ValueSerDeGenerator.get(context(), classOf(MockDataModel.class));
        loading(cl -> {
            ValueSerDe object = (ValueSerDe) gen.resolve(cl).newInstance();

            MockDataModel model = new MockDataModel();
            model.getKeyOption().modify(100);
            model.getSortOption().modify(new BigDecimal("3.14"));
            model.getValueOption().modify("Hello, world!");

            DataBuffer buffer = new DataBuffer();
            object.serialize(model, buffer);

            MockDataModel copy = (MockDataModel) object.deserialize(buffer);
            assertThat(buffer.getReadRemaining(), is(0));
            assertThat(copy, is(not(sameInstance(model))));
            assertThat(copy.getKeyOption(), is(model.getKeyOption()));
            assertThat(copy.getSortOption(), is(model.getSortOption()));
            assertThat(copy.getValueOption(), is(model.getValueOption()));
        });
    }

    /**
     * cache - equivalent.
     */
    @Test
    public void cache() {
        ClassData a = ValueSerDeGenerator.generate(context(), typeOf(MockDataModel.class));
        ClassData b = ValueSerDeGenerator.generate(context(), typeOf(MockDataModel.class));
        assertThat(b, is(cacheOf(a)));
    }

    /**
     * cache w/ different types.
     */
    @Test
    public void cache_diff_type() {
        ClassData a = ValueSerDeGenerator.generate(context(), typeOf(MockDataModel.class));
        ClassData b = ValueSerDeGenerator.generate(context(), typeOf(MockKeyValueModel.class));
        assertThat(b, is(not(cacheOf(a))));
    }
}
