/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.model.description;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ValueDescription.ValueKind;

/**
 * Test for {@link ArrayDescription}.
 */
public class ArrayDescriptionTest {

    /**
     * Test for {@link ArrayDescription#of(Object)} with primitive value array.
     * @throws Exception if failed
     */
    @Test
    public void of_primitives() throws Exception {
        int[] array = { 1, 2, 3 };
        ArrayDescription desc = ArrayDescription.of(array);
        assertThat(desc.getValueKind(), is(ValueKind.ARRAY));
        assertThat(desc.getValueType().resolve(getClass().getClassLoader()), is((Object) int[].class));
        assertThat(desc.getElements(), hasSize(3));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) array));
    }

    /**
     * Test for {@link ArrayDescription#of(Object)} with object array.
     * @throws Exception if failed
     */
    @Test
    public void of_objects() throws Exception {
        String[] array = { "a", "b", "c" };
        ArrayDescription desc = ArrayDescription.of(array);
        assertThat(desc.getValueKind(), is(ValueKind.ARRAY));
        assertThat(desc.getValueType().resolve(getClass().getClassLoader()), is((Object) String[].class));
        assertThat(desc.getElements(), hasSize(3));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) array));
    }

    /**
     * Test for {@link ArrayDescription#of(Object)} with object array.
     * @throws Exception if failed
     */
    @Test
    public void of_objects_subtyping() throws Exception {
        CharSequence[] array = { "a", "b", "c" };
        ArrayDescription desc = ArrayDescription.of(array);
        assertThat(desc.getValueKind(), is(ValueKind.ARRAY));
        assertThat(desc.getValueType().resolve(getClass().getClassLoader()), is((Object) CharSequence[].class));
        assertThat(desc.getElements(), hasSize(3));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) array));
    }

    /**
     * Test for {@link ArrayDescription#of(Object)} with scalar value.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_scalar() throws Exception {
        ArrayDescription.of("invalid");
    }

    /**
     * test equalities.
     * @throws Exception if failed
     */
    @Test
    public void equality() throws Exception {
        ArrayDescription d0 = ArrayDescription.of(new String[] { "a", "b", "c" });
        ArrayDescription d1 = ArrayDescription.of(new String[] { "a", "b", "c" });
        ArrayDescription d2 = ArrayDescription.of(new String[] { "a", "b" });
        ArrayDescription d3 = ArrayDescription.of(new CharSequence[] { "a", "b", "c" });

        assertThat(d1.toString(), d1, is(d0));
        assertThat(d1.toString(), d1.hashCode(), is(d0.hashCode()));
        assertThat(d1.toString(), d2, is(not(d0)));
        assertThat(d1.toString(), d3, is(not(d0)));
    }
}
