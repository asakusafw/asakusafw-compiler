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

import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.runtime.io.util.DataBuffer;

/**
 * Test for {@link KeyValueSerDeGenerator}.
 */
@SuppressWarnings("deprecation")
public class KeyValueSerDeGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Group group = group("=key", "+sort");
        ClassDescription gen = KeyValueSerDeGenerator.get(context(), classOf(MockDataModel.class), group);
        loading(cl -> {
            KeyValueSerDe object = (KeyValueSerDe) gen.resolve(cl).newInstance();

            MockDataModel model = new MockDataModel();
            model.getKeyOption().modify(100);
            model.getSortOption().modify(new BigDecimal("3.14"));
            model.getValueOption().modify("Hello, world!");

            DataBuffer kBuffer = new DataBuffer();
            DataBuffer vBuffer = new DataBuffer();
            object.serializeKey(model, kBuffer);
            object.serializeValue(model, vBuffer);

            assertThat(kBuffer.getReadRemaining(), is(greaterThan(0)));
            assertThat(vBuffer.getReadRemaining(), is(greaterThan(0)));

            MockDataModel copy = (MockDataModel) object.deserializePair(kBuffer, vBuffer);
            assertThat(kBuffer.getReadRemaining(), is(0));
            assertThat(vBuffer.getReadRemaining(), is(0));
            assertThat(copy, is(not(sameInstance(model))));
            assertThat(copy.getKeyOption(), is(model.getKeyOption()));
            assertThat(copy.getSortOption(), is(model.getSortOption()));
            assertThat(copy.getValueOption(), is(model.getValueOption()));
        });
    }

    /**
     * keys and sorts.
     */
    @Test
    public void full() {
        Group group = group("=key", "+sort", "-value");
        ClassDescription gen = KeyValueSerDeGenerator.get(context(), classOf(MockDataModel.class), group);
        loading(cl -> {
            KeyValueSerDe object = (KeyValueSerDe) gen.resolve(cl).newInstance();

            MockDataModel model = new MockDataModel();
            model.getKeyOption().modify(100);
            model.getSortOption().modify(new BigDecimal("3.14"));
            model.getValueOption().modify("Hello, world!");

            DataBuffer kBuffer = new DataBuffer();
            DataBuffer vBuffer = new DataBuffer();
            object.serializeKey(model, kBuffer);
            object.serializeValue(model, vBuffer);

            assertThat(kBuffer.getReadRemaining(), is(greaterThan(0)));
            assertThat(vBuffer.getReadRemaining(), is(greaterThan(0)));

            MockDataModel copy = (MockDataModel) object.deserializePair(kBuffer, vBuffer);
            assertThat(kBuffer.getReadRemaining(), is(0));
            assertThat(vBuffer.getReadRemaining(), is(0));
            assertThat(copy, is(not(sameInstance(model))));
            assertThat(copy.getKeyOption(), is(model.getKeyOption()));
            assertThat(copy.getSortOption(), is(model.getSortOption()));
            assertThat(copy.getValueOption(), is(model.getValueOption()));
        });
    }

    /**
     * empty keys.
     */
    @Test
    public void empty_keys() {
        ClassDescription gen = KeyValueSerDeGenerator.get(context(), classOf(MockDataModel.class), group());
        loading(cl -> {
            KeyValueSerDe object = (KeyValueSerDe) gen.resolve(cl).newInstance();

            MockDataModel model = new MockDataModel();
            model.getKeyOption().modify(100);
            model.getSortOption().modify(new BigDecimal("3.14"));
            model.getValueOption().modify("Hello, world!");

            DataBuffer kBuffer = new DataBuffer();
            DataBuffer vBuffer = new DataBuffer();
            object.serializeKey(model, kBuffer);
            object.serializeValue(model, vBuffer);

            assertThat(kBuffer.getReadRemaining(), is(greaterThan(0)));
            assertThat(vBuffer.getReadRemaining(), is(greaterThan(0)));

            MockDataModel copy = (MockDataModel) object.deserializePair(kBuffer, vBuffer);
            assertThat(kBuffer.getReadRemaining(), is(0));
            assertThat(vBuffer.getReadRemaining(), is(0));
            assertThat(copy, is(not(sameInstance(model))));
            assertThat(copy.getKeyOption(), is(model.getKeyOption()));
            assertThat(copy.getSortOption(), is(model.getSortOption()));
            assertThat(copy.getValueOption(), is(model.getValueOption()));
        });
    }

    /**
     * empty values.
     */
    @Test
    public void empty_values() {
        Group group = group("=key", "=sort", "=value");
        ClassDescription gen = KeyValueSerDeGenerator.get(context(), classOf(MockDataModel.class), group);
        loading(cl -> {
            KeyValueSerDe object = (KeyValueSerDe) gen.resolve(cl).newInstance();

            MockDataModel model = new MockDataModel();
            model.getKeyOption().modify(100);
            model.getSortOption().modify(new BigDecimal("3.14"));
            model.getValueOption().modify("Hello, world!");

            DataBuffer kBuffer = new DataBuffer();
            DataBuffer vBuffer = new DataBuffer();
            object.serializeKey(model, kBuffer);
            object.serializeValue(model, vBuffer);

            assertThat(kBuffer.getReadRemaining(), is(greaterThan(0)));
            assertThat(vBuffer.getReadRemaining(), is(greaterThan(0)));

            MockDataModel copy = (MockDataModel) object.deserializePair(kBuffer, vBuffer);
            assertThat(kBuffer.getReadRemaining(), is(0));
            assertThat(vBuffer.getReadRemaining(), is(0));
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
        ClassData a = KeyValueSerDeGenerator.generate(context(), typeOf(MockDataModel.class), group("=key"));
        ClassData b = KeyValueSerDeGenerator.generate(context(), typeOf(MockDataModel.class), group("=key"));
        assertThat(b, is(cacheOf(a)));
    }

    /**
     * cache w/ different types.
     */
    @Test
    public void cache_diff_type() {
        ClassData a = KeyValueSerDeGenerator.generate(context(), typeOf(MockDataModel.class), group("=key"));
        ClassData b = KeyValueSerDeGenerator.generate(context(), typeOf(MockKeyValueModel.class), group("=key"));
        assertThat(b, is(not(cacheOf(a))));
    }

    /**
     * cache w/ different groupings.
     */
    @Test
    public void cache_diff_group() {
        ClassData a = KeyValueSerDeGenerator.generate(context(), typeOf(MockDataModel.class), group("=key", "+sort"));
        ClassData b = KeyValueSerDeGenerator.generate(context(), typeOf(MockDataModel.class), group("=key", "-sort"));
        assertThat(b, is(not(cacheOf(a))));
    }
}
