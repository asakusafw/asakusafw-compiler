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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.*;

import java.util.function.Supplier;

import org.junit.Test;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Test for {@link SupplierGenerator}.
 */
public class SupplierGeneratorTest extends ClassGeneratorTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ClassDescription gen = SupplierGenerator.get(context(), classOf(String.class));
        loading(cl -> {
            Supplier<?> object = (Supplier<?>) gen.resolve(cl).newInstance();
            Object o0 = object.get();
            Object o1 = object.get();
            assertThat(o0, instanceOf(String.class));
            assertThat(o1, instanceOf(String.class));
            assertThat(o0, is(not(sameInstance(o1))));
        });
    }


    /**
     * cache - equivalent.
     */
    @Test
    public void cache() {
        ClassData a = SupplierGenerator.generate(context(), typeOf(MockDataModel.class));
        ClassData b = SupplierGenerator.generate(context(), typeOf(MockDataModel.class));
        assertThat(b, is(cacheOf(a)));
    }

    /**
     * cache w/ different types.
     */
    @Test
    public void cache_diff_type() {
        ClassData a = SupplierGenerator.generate(context(), typeOf(MockDataModel.class));
        ClassData b = SupplierGenerator.generate(context(), typeOf(MockKeyValueModel.class));
        assertThat(b, is(not(cacheOf(a))));
    }
}
