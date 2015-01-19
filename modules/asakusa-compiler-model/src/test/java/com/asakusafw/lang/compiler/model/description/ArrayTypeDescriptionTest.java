package com.asakusafw.lang.compiler.model.description;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.TypeDescription.TypeKind;

/**
 * Test for {@link ArrayTypeDescription}.
 */
public class ArrayTypeDescriptionTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void of_primitive() throws Exception {
        ArrayTypeDescription desc = ArrayTypeDescription.of(int[].class);
        assertThat(desc.getTypeKind(), is(TypeKind.ARRAY));
        assertThat(desc.getComponentType(), is((Object) ReifiableTypeDescription.of(int.class)));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) int[].class));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void of_reference() throws Exception {
        ArrayTypeDescription desc = ArrayTypeDescription.of(String[].class);
        assertThat(desc.getComponentType(), is((Object) ReifiableTypeDescription.of(String.class)));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) String[].class));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_scalar() throws Exception {
        ArrayTypeDescription.of(int.class);
    }
}
