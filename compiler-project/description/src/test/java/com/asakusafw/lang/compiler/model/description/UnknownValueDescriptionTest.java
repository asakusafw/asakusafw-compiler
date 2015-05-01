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

import com.asakusafw.lang.compiler.model.description.ValueDescription.ValueKind;

/**
 * Test for {@link UnknownValueDescription}.
 */
public class UnknownValueDescriptionTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        UnknownValueDescription desc = UnknownValueDescription.of(this);
        assertThat(desc.getValueKind(), is(ValueKind.UNKNOWN));
        assertThat(desc.getValueType().getErasure().resolve(getClass().getClassLoader()), is((Object) getClass()));
        assertThat(desc.getLabel(), is(notNullValue()));
        try {
            desc.resolve(getClass().getClassLoader());
            fail();
        } catch (ReflectiveOperationException e) {
            // ok.
        }
    }
}
