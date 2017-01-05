/**
 * Copyright 2011-2017 Asakusa Framework Team.
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

import java.lang.annotation.ElementType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.asakusafw.lang.compiler.model.description.ValueDescription.ValueKind;

/**
 * Test for {@link Descriptions}.
 */
public class DescriptionsTest {

    /**
     * Test information collector.
     */
    @Rule
    public final TestWatcher collector = new TestWatcher() {
        @Override
        protected void starting(org.junit.runner.Description d) {
            DescriptionsTest.this.description = d;
        }
    };

    Description description;

    /**
     * null values.
     */
    @Test
    public void null_value() {
        ValueDescription desc = Descriptions.valueOf(null);
        assertThat(desc.getValueKind(), is(ValueKind.IMMEDIATE));
        assertThat(resolve(desc), is(nullValue()));
    }

    /**
     * immediate values.
     */
    @Test
    public void immediate() {
        ValueDescription desc = Descriptions.valueOf(100);
        assertThat(desc.getValueKind(), is(ValueKind.IMMEDIATE));
        assertThat(resolve(desc), is((Object) 100));
    }

    /**
     * enum constant values.
     */
    @Test
    public void enum_constant() {
        ValueDescription desc = Descriptions.valueOf(ElementType.FIELD);
        assertThat(desc.getValueKind(), is(ValueKind.ENUM_CONSTANT));
        assertThat(resolve(desc), is((Object) ElementType.FIELD));
    }

    /**
     * type values.
     */
    @Test
    public void type() {
        ValueDescription desc = Descriptions.valueOf(int.class);
        assertThat(desc.getValueKind(), is(ValueKind.TYPE));
        assertThat(resolve(desc), is((Object) int.class));
    }

    /**
     * annotation values.
     */
    @Test
    public void annotation() {
        Test annotation = description.getAnnotation(Test.class);
        ValueDescription desc = Descriptions.valueOf(annotation);
        assertThat(desc.getValueKind(), is(ValueKind.ANNOTATION));
        Test resolved = (Test) resolve(desc);
        assertThat(resolved.expected(), is((Object) annotation.expected()));
    }

    /**
     * serializable values.
     */
    @Test
    public void serializable() {
        ValueDescription desc = Descriptions.valueOf(new StringBuilder("testing"));
        assertThat(desc.getValueKind(), is(ValueKind.SERIALIZABLE));
        assertThat(resolve(desc).toString(), is("testing"));
    }

    /**
     * arrays.
     */
    @Test
    public void array() {
        ValueDescription desc = Descriptions.valueOf(new int[] { 1, 2, 3, });
        assertThat(desc.getValueKind(), is(ValueKind.ARRAY));
        assertThat(resolve(desc), is((Object) new int[] { 1, 2, 3, }));
    }

    /**
     * unknown values.
     */
    @Test
    public void unknown() {
        ValueDescription desc = Descriptions.valueOf(new Object());
        assertThat(desc.getValueKind(), is(ValueKind.UNKNOWN));
    }

    private Object resolve(ValueDescription value) {
        try {
            return value.resolve(getClass().getClassLoader());
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
