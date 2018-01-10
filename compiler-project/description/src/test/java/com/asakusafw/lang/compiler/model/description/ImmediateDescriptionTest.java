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

/**
 * Test for {@link ImmediateDescription}.
 */
public class ImmediateDescriptionTest {

    /**
     * simple case.
     */
    @Test
    public void of_boolean() {
        ImmediateDescription desc = ImmediateDescription.of(true);
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(boolean.class)));
        assertThat(desc.getValue(), is((Object) true));
    }

    /**
     * simple case.
     */
    @Test
    public void of_byte() {
        ImmediateDescription desc = ImmediateDescription.of((byte) 100);
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(byte.class)));
        assertThat(desc.getValue(), is((Object) (byte) 100));
    }

    /**
     * simple case.
     */
    @Test
    public void of_short() {
        ImmediateDescription desc = ImmediateDescription.of((short) 100);
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(short.class)));
        assertThat(desc.getValue(), is((Object) (short) 100));
    }

    /**
     * simple case.
     */
    @Test
    public void of_int() {
        ImmediateDescription desc = ImmediateDescription.of(100);
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(int.class)));
        assertThat(desc.getValue(), is((Object) 100));
    }

    /**
     * simple case.
     */
    @Test
    public void of_long() {
        ImmediateDescription desc = ImmediateDescription.of(100L);
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(long.class)));
        assertThat(desc.getValue(), is((Object) 100L));
    }

    /**
     * simple case.
     */
    @Test
    public void of_float() {
        ImmediateDescription desc = ImmediateDescription.of(100.f);
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(float.class)));
        assertThat(desc.getValue(), is((Object) 100.f));
    }

    /**
     * simple case.
     */
    @Test
    public void of_double() {
        ImmediateDescription desc = ImmediateDescription.of(100.d);
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(double.class)));
        assertThat(desc.getValue(), is((Object) 100.d));
    }

    /**
     * simple case.
     */
    @Test
    public void of_char() {
        ImmediateDescription desc = ImmediateDescription.of('A');
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(char.class)));
        assertThat(desc.getValue(), is((Object) 'A'));
    }

    /**
     * simple case.
     */
    @Test
    public void of_string() {
        ImmediateDescription desc = ImmediateDescription.of("testing");
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(String.class)));
        assertThat(desc.getValue(), is((Object) "testing"));
    }

    /**
     * {@link ImmediateDescription#of(Object)} with basic type.
     */
    @Test
    public void of_object_primitive() {
        ImmediateDescription desc = ImmediateDescription.of((Object) 100);
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(int.class)));
        assertThat(desc.getValue(), is((Object) 100));
    }

    /**
     * {@link ImmediateDescription#of(Object)} with string type.
     */
    @Test
    public void of_object_string() {
        ImmediateDescription desc = ImmediateDescription.of((Object) "testing");
        assertThat(desc.getValueType(), is((TypeDescription) ReifiableTypeDescription.of(String.class)));
        assertThat(desc.getValue(), is((Object) "testing"));
    }

    /**
     * {@link ImmediateDescription#of(Object)} with null.
     */
    @Test
    public void of_object_null() {
        ImmediateDescription desc = ImmediateDescription.of((Object) null);
        assertThat(desc.getValue(), is(nullValue()));
    }

    /**
     * {@link ImmediateDescription#of(Object)} with unsupported type.
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_object_unsupported() {
        ImmediateDescription.of(new Object());
    }
}
