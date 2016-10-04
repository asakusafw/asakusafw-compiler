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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Test for {@link ClassNameMap}.
 */
public class ClassNameMapTest {

    private final ClassNameMap map = new ClassNameMap("com.example.");

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ClassDescription desc = map.get("a", "A");
        assertThat(desc, is(valid()));
        assertThat(desc.getBinaryName(), startsWith("com.example.a.A_"));
    }

    /**
     * unique.
     */
    @Test
    public void unique() {
        ClassDescription a0 = map.get("a", "A");
        ClassDescription a1 = map.get("a", "B");
        ClassDescription a2 = map.get("a", "C");
        assertThat(a0, is(valid()));
        assertThat(a1, is(valid()));
        assertThat(a2, is(valid()));
        assertThat(a0, is(not(a1)));
        assertThat(a0, is(not(a2)));
        assertThat(a1, is(not(a2)));
    }

    /**
     * w/o hint.
     */
    @Test
    public void category_null() {
        ClassDescription desc = map.get(null, "A");
        assertThat(desc, is(valid()));
        assertThat(desc.getBinaryName(), startsWith("com.example."));
    }

    /**
     * w/o hint.
     */
    @Test
    public void hint_null() {
        ClassDescription desc = map.get("a", null);
        assertThat(desc, is(valid()));
        assertThat(desc.getBinaryName(), startsWith("com.example.a."));
    }

    /**
     * w/ invalid category.
     */
    @Test
    public void category_invalid() {
        ClassDescription desc = map.get("<>", "A");
        assertThat(desc, is(valid()));
        assertThat(desc.getBinaryName(), startsWith("com.example."));
    }

    /**
     * w/ invalid hint.
     */
    @Test
    public void hint_invalid() {
        ClassDescription desc = map.get("a", "<>");
        assertThat(desc, is(valid()));
        assertThat(desc.getBinaryName(), startsWith("com.example.a."));
    }

    private static Matcher<ClassDescription> valid() {
        return new BaseMatcher<ClassDescription>() {
            @Override
            public boolean matches(Object item) {
                String name = ((ClassDescription) item).getClassName();
                return name.matches("[A-Za-z_]\\w*(\\.[A-Za-z_]\\w*)*");
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("valid class name");
            }
        };
    }
}
