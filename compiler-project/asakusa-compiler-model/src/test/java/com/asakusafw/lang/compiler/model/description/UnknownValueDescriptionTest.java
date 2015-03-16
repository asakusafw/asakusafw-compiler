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
        assertThat(desc.getValueType().resolve(getClass().getClassLoader()), is((Object) getClass()));
        assertThat(desc.getLabel(), is(notNullValue()));
        try {
            desc.resolve(getClass().getClassLoader());
            fail();
        } catch (ReflectiveOperationException e) {
            // ok.
        }
    }
}
