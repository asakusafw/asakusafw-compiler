/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
 * Test for {@link ClassDescription}.
 */
public class ClassDescriptionTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ClassDescription desc = new ClassDescription("com.example.Testing");
        assertThat(desc.getTypeKind(), is(TypeKind.CLASS));
        assertThat(desc.getClassName(), is("com.example.Testing"));
        assertThat(desc.getBinaryName(), is("com.example.Testing"));
        assertThat(desc.getInternalName(), is("com/example/Testing"));
        assertThat(desc.getSimpleName(), is("Testing"));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void of() throws Exception {
        ClassDescription desc = ClassDescription.of(String.class);
        assertThat(desc.getClassName(), is("java.lang.String"));
        assertThat(desc.getBinaryName(), is("java.lang.String"));
        assertThat(desc.getInternalName(), is("java/lang/String"));
        assertThat(desc.getSimpleName(), is("String"));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) String.class));
    }

    /**
     * inner class.
     * @throws Exception if failed
     */
    @Test
    public void of_inner() throws Exception {
        ClassDescription desc = ClassDescription.of(Thread.State.class);
        assertThat(desc.getClassName(), is("java.lang.Thread.State"));
        assertThat(desc.getBinaryName(), is("java.lang.Thread$State"));
        assertThat(desc.getInternalName(), is("java/lang/Thread$State"));
        assertThat(desc.getSimpleName(), is("State"));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) Thread.State.class));
    }

    /**
     * pass primitive type.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_primitive() throws Exception {
        ClassDescription.of(int.class);
    }

    /**
     * pass array type.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_array() throws Exception {
        ClassDescription.of(String[].class);
    }
}
