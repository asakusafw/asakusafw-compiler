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
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(boolean.class)));
        assertThat(desc.getValue(), is((Object) true));
    }

    /**
     * simple case.
     */
    @Test
    public void of_byte() {
        ImmediateDescription desc = ImmediateDescription.of((byte) 100);
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(byte.class)));
        assertThat(desc.getValue(), is((Object) (byte) 100));
    }

    /**
     * simple case.
     */
    @Test
    public void of_short() {
        ImmediateDescription desc = ImmediateDescription.of((short) 100);
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(short.class)));
        assertThat(desc.getValue(), is((Object) (short) 100));
    }

    /**
     * simple case.
     */
    @Test
    public void of_int() {
        ImmediateDescription desc = ImmediateDescription.of(100);
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(int.class)));
        assertThat(desc.getValue(), is((Object) 100));
    }

    /**
     * simple case.
     */
    @Test
    public void of_long() {
        ImmediateDescription desc = ImmediateDescription.of(100L);
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(long.class)));
        assertThat(desc.getValue(), is((Object) 100L));
    }

    /**
     * simple case.
     */
    @Test
    public void of_float() {
        ImmediateDescription desc = ImmediateDescription.of(100.f);
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(float.class)));
        assertThat(desc.getValue(), is((Object) 100.f));
    }

    /**
     * simple case.
     */
    @Test
    public void of_double() {
        ImmediateDescription desc = ImmediateDescription.of(100.d);
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(double.class)));
        assertThat(desc.getValue(), is((Object) 100.d));
    }

    /**
     * simple case.
     */
    @Test
    public void of_char() {
        ImmediateDescription desc = ImmediateDescription.of('A');
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(char.class)));
        assertThat(desc.getValue(), is((Object) 'A'));
    }

    /**
     * simple case.
     */
    @Test
    public void of_string() {
        ImmediateDescription desc = ImmediateDescription.of("testing");
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(String.class)));
        assertThat(desc.getValue(), is((Object) "testing"));
    }

    /**
     * {@link ImmediateDescription#of(Object)} with basic type.
     */
    @Test
    public void of_object_primitive() {
        ImmediateDescription desc = ImmediateDescription.of((Object) 100);
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(int.class)));
        assertThat(desc.getValue(), is((Object) 100));
    }

    /**
     * {@link ImmediateDescription#of(Object)} with string type.
     */
    @Test
    public void of_object_string() {
        ImmediateDescription desc = ImmediateDescription.of((Object) "testing");
        assertThat(desc.getValueType(), is(ReifiableTypeDescription.of(String.class)));
        assertThat(desc.getValue(), is((Object) "testing"));
    }

    /**
     * {@link ImmediateDescription#of(Object)} with unsupported type.
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_object_unsupported() {
        ImmediateDescription.of(new Object());
    }

    /**
     * {@link ImmediateDescription#of(Object)} with unsupported type.
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_object_null() {
        ImmediateDescription.of((Object) null);
    }
}
