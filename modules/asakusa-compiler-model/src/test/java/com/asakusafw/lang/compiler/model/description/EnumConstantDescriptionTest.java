package com.asakusafw.lang.compiler.model.description;

import static org.hamcrest.CoreMatchers.*;
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
}
