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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ValueDescription.ValueKind;

/**
 * Test for {@link SerializableValueDescription}.
 */
public class SerializableValueDescriptionTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        SerializableValueDescription desc = SerializableValueDescription.of(new StringBuilder("testing"));
        assertThat(desc.getValueKind(), is(ValueKind.SERIALIZABLE));
        assertThat(desc.getValueType().getErasure().resolve(getClass().getClassLoader()), is((Object) StringBuilder.class));
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(desc.getSerialized()))) {
            Object read = in.readObject();
            assertThat(read, is(instanceOf(StringBuilder.class)));
            assertThat(read.toString(), is("testing"));
        }
        Object read = desc.resolve(getClass().getClassLoader());
        assertThat(read, is(instanceOf(StringBuilder.class)));
        assertThat(read.toString(), is("testing"));
    }

    /**
     * test for custom serializable objects.
     * @throws Exception if failed
     */
    @Test
    public void custom() throws Exception {
        SerializableValueDescription desc = SerializableValueDescription.of(new SerializableWithArray(1, 2, 3));
        assertThat(desc.getValueKind(), is(ValueKind.SERIALIZABLE));
        assertThat(desc.getValueType().getErasure().resolve(getClass().getClassLoader()), is((Object) SerializableWithArray.class));
        Object read = desc.resolve(getClass().getClassLoader());
        assertThat(read, is(instanceOf(SerializableWithArray.class)));
        assertThat(read, is(equalTo((Object) new SerializableWithArray(1, 2, 3))));
    }

    /**
     * non-serializable objects.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_nonserializable() throws Exception {
        SerializableValueDescription.of(new Object());
    }

    /**
     * test equalities.
     * @throws Exception if failed
     */
    @Test
    public void equality() throws Exception {
        SerializableValueDescription d0 = SerializableValueDescription.of(new SerializableWithArray(1, 2, 3));
        SerializableValueDescription d1 = new SerializableValueDescription(d0.getValueType(), d0.getSerialized());
        SerializableValueDescription d2 = SerializableValueDescription.of(new SerializableWithArray(2, 3, 4));
        SerializableValueDescription d3 = SerializableValueDescription.of(new int[] { 1, 2, 3 });

        assertThat(d1.toString(), d1, is(d0));
        assertThat(d1.toString(), d1.hashCode(), is(d0.hashCode()));
        assertThat(d1.toString(), d2, is(not(d0)));
        assertThat(d1.toString(), d3, is(not(d0)));
    }

    @SuppressWarnings("javadoc")
    public static final class SerializableWithArray implements Serializable {

        private static final long serialVersionUID = -4292530364584224096L;

        final String[] array;

        public SerializableWithArray() {
            this.array = null;
        }

        public SerializableWithArray(int... array) {
            this.array = new String[array.length];
            for (int i = 0; i < array.length; i++) {
                this.array[i] = String.valueOf(array[i]);
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(array);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            SerializableWithArray other = (SerializableWithArray) obj;
            if (!Arrays.equals(array, other.array)) {
                return false;
            }
            return true;
        }
    }
}
