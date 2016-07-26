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
package com.asakusafw.lang.compiler.model.description;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

/**
 * Test for {@link MethodDescription}.
 */
public class MethodDescriptionTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        // Intger.valueOf(String, int)
        MethodDescription desc = new MethodDescription(
                classOf(Integer.class),
                "valueOf",
                Arrays.asList(typeOf(String.class), typeOf(int.class)));
        assertThat(desc.getDeclaringClass(), is(classOf(Integer.class)));
        assertThat(desc.getName(), is("valueOf"));
        assertThat(desc.getParameterTypes(), contains(typeOf(String.class), typeOf(int.class)));
    }

    /**
     * resolving method.
     * @throws Exception if failed
     */
    @Test
    public void resolve() throws Exception {
        // Intger.valueOf(String, int)
        MethodDescription desc = new MethodDescription(
                classOf(Integer.class),
                "valueOf",
                Arrays.asList(typeOf(String.class), typeOf(int.class)));

        Method method = desc.resolve(getClass().getClassLoader());
        assertThat(method.invoke(null, "1f", 16), is((Object) 31));

        MethodDescription restored = MethodDescription.of(method);
        assertThat(restored, is(desc));
    }

    /**
     * resolving method.
     * @throws Exception if failed
     */
    @Test
    public void resolve_inherited() throws Exception {
        // Intger.intValue()
        MethodDescription desc = new MethodDescription(
                classOf(Integer.class),
                "intValue",
                Collections.emptyList());

        Method method = desc.resolve(getClass().getClassLoader());
        assertThat(method.invoke(100), is((Object) 100));
    }

    /**
     * resolving method.
     * @throws Exception if failed
     */
    @Test
    public void resolve_non_public() throws Exception {
        // Intger.intValue()
        MethodDescription desc = new MethodDescription(
                classOf(getClass()),
                "non_public",
                Collections.emptyList());

        Method method = desc.resolve(getClass().getClassLoader());
        assertThat(method.invoke(null), is((Object) "non public"));
    }

    /**
     * method for testing.
     * @return just {@code "non public"}
     */
    static String non_public() {
        return "non public";
    }

    /**
     * equality check.
     * @throws Exception if failed
     */
    @Test
    public void equality() throws Exception {
        // Intger.valueOf(String, int)
        MethodDescription d0 = new MethodDescription(
                classOf(Integer.class),
                "valueOf",
                Arrays.asList(typeOf(String.class), typeOf(int.class)));

        // equivalent to d0
        MethodDescription d1 = new MethodDescription(
                classOf(Integer.class),
                "valueOf",
                Arrays.asList(typeOf(String.class), typeOf(int.class)));

        // Intger.valueOf(String)
        MethodDescription d2 = new MethodDescription(
                classOf(Integer.class),
                "valueOf",
                Arrays.asList(typeOf(String.class)));

        // Long.valueOf(String, int)
        MethodDescription d3 = new MethodDescription(
                classOf(Long.class),
                "valueOf",
                Arrays.asList(typeOf(String.class), typeOf(int.class)));

        assertThat(d1.toString(), d1, is(d0));
        assertThat(d0.hashCode(), is(d1.hashCode()));
        assertThat(d2.toString(), d2, is(not(d0)));
        assertThat(d3.toString(), d3, is(not(d0)));
    }
}
