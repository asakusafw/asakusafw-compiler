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
package com.asakusafw.lang.compiler.analyzer.util;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Type;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.Test;

/**
 * Test for {@link TypeInfo}.
 */
public class TypeInfoTest {

    /**
     * invoke {@code Object}.
     */
    @Test
    public void invoke_object() {
        List<Type> invoked = TypeInfo.invoke(Object.class, String.class);
        assertThat(invoked.size(), is(0));
    }

    /**
     * invoke direct parent.
     */
    @Test
    public void invoke_parent() {
        List<Type> invoked = TypeInfo.invoke(ArrayList.class, StringList.class);
        assertThat(invoked.size(), is(1));
        assertThat(invoked.get(0), is((Type) String.class));
    }

    /**
     * invoke ancestor.
     */
    @Test
    public void invoke_ancestor() {
        List<Type> invoked = TypeInfo.invoke(AbstractCollection.class, StringList.class);
        assertThat(invoked.size(), is(1));
        assertThat(invoked.get(0), is((Type) String.class));
    }

    /**
     * invoke interface.
     */
    @Test
    public void invoke_interface() {
        List<Type> invoked = TypeInfo.invoke(List.class, StringList.class);
        assertThat(invoked.size(), is(1));
        assertThat(invoked.get(0), is((Type) String.class));
    }

    /**
     * invoke ancestor interface.
     */
    @Test
    public void invoke_interface_ancestor() {
        List<Type> invoked = TypeInfo.invoke(Collection.class, StringList.class);
        assertThat(invoked.size(), is(1));
        assertThat(invoked.get(0), is((Type) String.class));
    }

    /**
     * invoke interface from interface.
     */
    @Test
    public void invoke_interface_from_interface() {
        List<Type> invoked = TypeInfo.invoke(List.class, IStringList.class);
        assertThat(invoked.size(), is(1));
        assertThat(invoked.get(0), is((Type) String.class));
    }

    /**
     * invoke ancestor interface from interface.
     */
    @Test
    public void invoke_interface_ancestor_from_interface() {
        List<Type> invoked = TypeInfo.invoke(Collection.class, IStringList.class);
        assertThat(invoked.size(), is(1));
        assertThat(invoked.get(0), is((Type) String.class));
    }

    /**
     * invoke unrelated.
     */
    @Test
    public void invoke_unrelated() {
        List<Type> invoked = TypeInfo.invoke(Set.class, IStringList.class);
        assertThat(invoked, is(nullValue()));
    }

    /**
     * erase raw.
     */
    @Test
    public void erase_raw() {
        assertThat(TypeInfo.erase(String.class), is((Object) String.class));
    }

    /**
     * erase parameterized.
     */
    @Test
    public void erase_parameterized() {
        assertThat(TypeInfo.erase(StringList.class.getGenericSuperclass()), is((Object) ArrayList.class));
    }

    /**
     * of raw.
     */
    @Test
    public void of_raw() {
        TypeInfo info = TypeInfo.of(String.class);
        assertThat(info.toString(), info.getRawType(), is((Object) String.class));
        assertThat(info.getTypeArguments(), is(empty()));
    }

    /**
     * of raw.
     */
    @Test
    public void of_parameterized() {
        TypeInfo info = TypeInfo.of(StringList.class.getGenericSuperclass());
        assertThat(info.toString(), info.getRawType(), is((Object) ArrayList.class));
        assertThat(info.getTypeArguments(), contains((Object) String.class));
        assertThat(info.getErasedTypeArguments(), contains((Object) String.class));
    }

    /**
     * of build.
     */
    @Test
    public void of_build() {
        TypeInfo info = TypeInfo.of(List.class, Integer.class);
        assertThat(info.toString(), info.getRawType(), is((Object) List.class));
        assertThat(info.getTypeArguments(), contains((Object) Integer.class));
        assertThat(info.getErasedTypeArguments(), contains((Object) Integer.class));
    }

    private static class StringList extends ArrayList<String> {

        private static final long serialVersionUID = 1L;
    }

    private interface IStringList extends List<String> {

        // no members
    }
}
