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
import com.asakusafw.lang.compiler.model.description.ValueDescription.ValueKind;

/**
 * Test for {@link ReifiableTypeDescription}.
 */
public class ReifiableTypeDescriptionTest {

    /**
     * basic types.
     * @throws Exception if failed
     */
    @Test
    public void of_basic() throws Exception {
        ReifiableTypeDescription desc = ReifiableTypeDescription.of(int.class);
        assertThat(desc.getValueKind(), is((Object) ValueKind.TYPE));
        assertThat(desc.getValueType().getErasure().resolve(getClass().getClassLoader()), is((Object) Class.class));
        assertThat(desc.getTypeKind(), is(TypeKind.BASIC));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) int.class));
    }

    /**
     * reference types.
     * @throws Exception if failed
     */
    @Test
    public void of_class() throws Exception {
        ReifiableTypeDescription desc = ReifiableTypeDescription.of(String.class);
        assertThat(desc.getTypeKind(), is(TypeKind.CLASS));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) String.class));
    }

    /**
     * array types.
     * @throws Exception if failed
     */
    @Test
    public void of_array() throws Exception {
        ReifiableTypeDescription desc = ReifiableTypeDescription.of(int[].class);
        assertThat(desc.getTypeKind(), is(TypeKind.ARRAY));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) int[].class));
    }
}
