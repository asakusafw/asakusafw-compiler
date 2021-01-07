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

import java.lang.annotation.ElementType;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ValueDescription.ValueKind;

/**
 * Test for {@link EnumConstantDescription}.
 */
public class EnumConstantDescriptionTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void of() throws Exception {
        EnumConstantDescription desc = EnumConstantDescription.of(ElementType.METHOD);
        assertThat(desc.getValueKind(), is(ValueKind.ENUM_CONSTANT));
        assertThat(desc.getValueType().resolve(getClass().getClassLoader()), is((Object) ElementType.class));
        assertThat(desc.getName(), is("METHOD"));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) ElementType.METHOD));
    }

    /**
     * test equalities.
     * @throws Exception if failed
     */
    @Test
    public void equality() throws Exception {
        EnumConstantDescription d0 = EnumConstantDescription.of(ValueKind.TYPE);
        EnumConstantDescription d1 = EnumConstantDescription.of(ValueKind.TYPE);
        EnumConstantDescription d2 = EnumConstantDescription.of(ValueKind.IMMEDIATE);
        EnumConstantDescription d3 = EnumConstantDescription.of(ElementType.TYPE);

        assertThat(d1.toString(), d1, is(d0));
        assertThat(d1.toString(), d1.hashCode(), is(d0.hashCode()));
        assertThat(d1.toString(), d2, is(not(d0)));
        assertThat(d1.toString(), d3, is(not(d0)));
    }
}
