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
