package com.asakusafw.lang.compiler.model.description;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.BasicTypeDescription.BasicTypeKind;
import com.asakusafw.lang.compiler.model.description.TypeDescription.TypeKind;

/**
 * Test for {@link BasicTypeDescription}.
 */
public class BasicTypeDescriptionTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void of_primitive() throws Exception {
        BasicTypeDescription desc = BasicTypeDescription.of(int.class);
        assertThat(desc.getTypeKind(), is(TypeKind.BASIC));
        assertThat(desc.getBasicTypeKind(), is(BasicTypeKind.INT));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) int.class));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void of_void() throws Exception {
        BasicTypeDescription desc = BasicTypeDescription.of(void.class);
        assertThat(desc.getBasicTypeKind(), is(BasicTypeKind.VOID));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) void.class));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_reference() throws Exception {
        BasicTypeDescription.of(String.class);
    }
}
