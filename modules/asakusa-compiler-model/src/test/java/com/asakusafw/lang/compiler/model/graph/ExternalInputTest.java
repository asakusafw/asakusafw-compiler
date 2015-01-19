package com.asakusafw.lang.compiler.model.graph;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.ExternalPort.PortKind;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;

/**
 * Test for {@link ExternalInput}.
 */
public class ExternalInputTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ExternalInput operator = ExternalInput.newInstance(
                "in", classOf(StringBuilder.class), typeOf(String.class));
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.INPUT));
        assertThat(operator.getPortKind(), is(PortKind.INPUT));
        assertThat(operator.getName(), is("in"));
        assertThat(operator.getOperatorPort().getName(), is(ExternalInput.PORT_NAME));
        assertThat(operator.getOperatorPort().getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getDescriptionClass(), is(classOf(StringBuilder.class)));
    }

    /**
     * test for copy.
     */
    @Test
    public void copy() {
        ExternalInput operator = ExternalInput.newInstance("in", typeOf(String.class));
        ExternalInput copy = operator.copy();
        assertThat(copy.toString(), copy.getOperatorKind(), is(OperatorKind.INPUT));
        assertThat(copy.getPortKind(), is(PortKind.INPUT));
        assertThat(copy.getName(), is("in"));
        assertThat(copy.getOperatorPort().getName(), is(ExternalInput.PORT_NAME));
        assertThat(copy.getOperatorPort().getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.getDataType(), is((Object) typeOf(String.class)));
        assertThat(copy.getDescriptionClass(), is(nullValue()));
    }
}
