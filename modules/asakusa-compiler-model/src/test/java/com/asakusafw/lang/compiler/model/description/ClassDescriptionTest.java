package com.asakusafw.lang.compiler.model.description;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.TypeDescription.TypeKind;

/**
 * Test for {@link ClassDescription}.
 */
public class ClassDescriptionTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ClassDescription desc = new ClassDescription("com.example.Testing");
        assertThat(desc.getTypeKind(), is(TypeKind.CLASS));
        assertThat(desc.getName(), is("com.example.Testing"));
        assertThat(desc.getSimpleName(), is("Testing"));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void of() throws Exception {
        ClassDescription desc = ClassDescription.of(String.class);
        assertThat(desc.getName(), is("java.lang.String"));
        assertThat(desc.getSimpleName(), is("String"));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) String.class));
    }

    /**
     * inner class.
     * @throws Exception if failed
     */
    @Test
    public void of_inner() throws Exception {
        ClassDescription desc = ClassDescription.of(Thread.State.class);
        assertThat(desc.getName(), is("java.lang.Thread$State"));
        assertThat(desc.getSimpleName(), is("State"));
        assertThat(desc.resolve(getClass().getClassLoader()), is((Object) Thread.State.class));
    }

    /**
     * pass primitive type.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_primitive() throws Exception {
        ClassDescription.of(int.class);
    }

    /**
     * pass array type.
     * @throws Exception if failed
     */
    @Test(expected = IllegalArgumentException.class)
    public void of_array() throws Exception {
        ClassDescription.of(String[].class);
    }
}
