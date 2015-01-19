package com.asakusafw.lang.compiler.model.description;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ValueDescription.ValueKind;

/**
 * Test for {@link ArrayDescription}.
 */
public class ArrayDescriptionTest {

    /**
     * Test for {@link ArrayDescription#of(Object)} with primitive value array.
     * @throws Exception if failed
     */
    @Test
    public void of_primitives() throws Exception {
        int[] array = { 1, 2, 3 };
        ArrayDescription desc = ArrayDescription.of(array);
        assertThat(desc.getValueKind(), is(ValueKind.ARRAY));
        assertThat(desc.getValueType().resolve(getClass().getClassLoader()), is((Object) int[].class));
        assertThat(desc.getElements(), hasSize(3));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) array));
    }

    /**
     * Test for {@link ArrayDescription#of(Object)} with object array.
     * @throws Exception if failed
     */
    @Test
    public void of_objects() throws Exception {
        String[] array = { "a", "b", "c" };
        ArrayDescription desc = ArrayDescription.of(array);
        assertThat(desc.getValueKind(), is(ValueKind.ARRAY));
        assertThat(desc.getValueType().resolve(getClass().getClassLoader()), is((Object) String[].class));
        assertThat(desc.getElements(), hasSize(3));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) array));
    }

    /**
     * Test for {@link ArrayDescription#of(Object)} with object array.
     * @throws Exception if failed
     */
    @Test
    public void of_objects_subtyping() throws Exception {
        CharSequence[] array = { "a", "b", "c" };
        ArrayDescription desc = ArrayDescription.of(array);
        assertThat(desc.getValueKind(), is(ValueKind.ARRAY));
        assertThat(desc.getValueType().resolve(getClass().getClassLoader()), is((Object) CharSequence[].class));
        assertThat(desc.getElements(), hasSize(3));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) array));
    }

    /**
     * Test for {@link ArrayDescription#of(Object)} with scalar value.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_scalar() throws Exception {
        ArrayDescription.of("invalid");
    }
}
