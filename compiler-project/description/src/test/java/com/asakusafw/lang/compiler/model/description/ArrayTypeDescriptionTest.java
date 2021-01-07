/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import com.asakusafw.lang.compiler.model.description.TypeDescription.TypeKind;

/**
 * Test for {@link ArrayTypeDescription}.
 */
public class ArrayTypeDescriptionTest {

    /**
     * for primitive arrays.
     * @throws Exception if failed
     */
    @Test
    public void of_primitive() throws Exception {
        ArrayTypeDescription desc = ArrayTypeDescription.of(int[].class);
        assertThat(desc.getTypeKind(), is(TypeKind.ARRAY));
        assertThat(desc.getComponentType(), is((Object) ReifiableTypeDescription.of(int.class)));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) int[].class));
    }

    /**
     * for reference arrays.
     * @throws Exception if failed
     */
    @Test
    public void of_reference() throws Exception {
        ArrayTypeDescription desc = ArrayTypeDescription.of(String[].class);
        assertThat(desc.getComponentType(), is((Object) ReifiableTypeDescription.of(String.class)));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) String[].class));
    }

    /**
     * for scalar type.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_scalar() throws Exception {
        ArrayTypeDescription.of(int.class);
    }

    /**
     * test equalities.
     * @throws Exception if failed
     */
    @Test
    public void equality() throws Exception {
        ArrayTypeDescription d0 = ArrayTypeDescription.of(int[].class);
        ArrayTypeDescription d1 = ArrayTypeDescription.of(int[].class);
        ArrayTypeDescription d2 = ArrayTypeDescription.of(long[].class);
        ArrayTypeDescription d3 = ArrayTypeDescription.of(int[][].class);

        assertThat(d1.toString(), d1, is(d0));
        assertThat(d1.toString(), d1.hashCode(), is(d0.hashCode()));
        assertThat(d1.toString(), d2, is(not(d0)));
        assertThat(d1.toString(), d3, is(not(d0)));
    }
}
