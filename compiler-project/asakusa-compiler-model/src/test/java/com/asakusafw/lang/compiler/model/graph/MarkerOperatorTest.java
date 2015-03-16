package com.asakusafw.lang.compiler.model.graph;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.annotation.ElementType;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;

/**
 * Test for {@link MarkerOperator}.
 */
public class MarkerOperatorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MarkerOperator operator = MarkerOperator.builder(typeOf(String.class))
                .attribute(ElementType.class, ElementType.TYPE)
                .build();
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.MARKER));
        assertThat(operator.getProperties(), hasSize(2));
        assertThat(operator.getInput().getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getInput().getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.getOutput().getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getOutput().getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.getAttributeTypes(), hasSize(1));
        assertThat(operator.getAttribute(ElementType.class), is(ElementType.TYPE));
        assertThat(operator.getAttribute(Dummy.class), is(nullValue()));
    }

    /**
     * test for copy.
     */
    @Test
    public void copy() {
        MarkerOperator operator = MarkerOperator.builder(typeOf(String.class))
                .attribute(ElementType.class, ElementType.METHOD)
                .attribute(Dummy.class, new Dummy(1))
                .build();
        MarkerOperator copy = operator.copy();
        assertThat(copy.toString(), copy, is(not(sameInstance(operator))));
        assertThat(copy.getProperties(), hasSize(2));
        assertThat(copy.getInput().getDataType(), is((Object) typeOf(String.class)));
        assertThat(copy.getInput().getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.getOutput().getDataType(), is((Object) typeOf(String.class)));
        assertThat(copy.getOutput().getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.getAttributeTypes(), hasSize(2));
        assertThat(copy.getAttribute(ElementType.class), is(ElementType.METHOD));
        assertThat(copy.getAttribute(Dummy.class).value, is(2));
    }

    private static final class Dummy implements OperatorAttribute {

        final int value;

        public Dummy(int value) {
            this.value = value;
        }

        @Override
        public OperatorAttribute copy() {
            return new Dummy(value + 1);
        }
    }
}
