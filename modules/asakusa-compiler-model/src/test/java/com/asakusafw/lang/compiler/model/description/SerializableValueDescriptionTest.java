package com.asakusafw.lang.compiler.model.description;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

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
        assertThat(desc.getValueType().resolve(getClass().getClassLoader()), is((Object) StringBuilder.class));
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
     * simple case.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_nonserializable() throws Exception {
        SerializableValueDescription.of(new Object());
    }
}
