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
package com.asakusafw.lang.compiler.model.description;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.BasicTypeDescription.BasicTypeKind;
import com.asakusafw.lang.compiler.model.description.TypeDescription.TypeKind;

/**
 * Test for {@link BasicTypeDescription}.
 */
public class BasicTypeDescriptionTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void of_primitive() throws Exception {
        BasicTypeDescription desc = BasicTypeDescription.of(int.class);
        assertThat(desc.getTypeKind(), is(TypeKind.BASIC));
        assertThat(desc.getBasicTypeKind(), is(BasicTypeKind.INT));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) int.class));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void of_void() throws Exception {
        BasicTypeDescription desc = BasicTypeDescription.of(void.class);
        assertThat(desc.getBasicTypeKind(), is(BasicTypeKind.VOID));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) void.class));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_reference() throws Exception {
        BasicTypeDescription.of(String.class);
    }

    /**
     * using keyword.
     * @throws Exception if failed
     */
    @Test
    public void keyword() throws Exception {
        assertThat(new BasicTypeDescription(BasicTypeKind.of("int")), is(BasicTypeDescription.of(int.class)));
        assertThat(new BasicTypeDescription(BasicTypeKind.of("double")), is(BasicTypeDescription.of(double.class)));
        assertThat(new BasicTypeDescription(BasicTypeKind.of("boolean")), is(BasicTypeDescription.of(boolean.class)));
    }
}
