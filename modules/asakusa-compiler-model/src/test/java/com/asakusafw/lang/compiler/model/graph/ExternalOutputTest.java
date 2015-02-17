package com.asakusafw.lang.compiler.model.graph;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalPort.PortKind;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Test for {@link ExternalOutput}.
 */
public class ExternalOutputTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ExternalOutputInfo info = new ExternalOutputInfo.Basic(
                new ClassDescription("Dummy"),
                "testing",
                classOf(String.class));

        ExternalOutput operator = ExternalOutput.newInstance("out", info);
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.OUTPUT));
        assertThat(operator.getPortKind(), is(PortKind.OUTPUT));
        assertThat(operator.getName(), is("out"));
        assertThat(operator.getOperatorPort().getName(), is(ExternalOutput.PORT_NAME));
        assertThat(operator.getOperatorPort().getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getInfo(), is(info));
    }

    /**
     * w/ upstream.
     */
    @Test
    public void w_upstream() {
        ExternalInput upstream = ExternalInput.newInstance("in", typeOf(String.class));
        ExternalOutput operator = ExternalOutput.newInstance("out", upstream.getOperatorPort());
        assertThat(operator.toString(), operator.getOperatorKind(), is(OperatorKind.OUTPUT));
        assertThat(operator.getPortKind(), is(PortKind.OUTPUT));
        assertThat(operator.getName(), is("out"));
        assertThat(operator.getOperatorPort().getName(), is(ExternalOutput.PORT_NAME));
        assertThat(operator.getOperatorPort().getOwner(), is(sameInstance((Object) operator)));
        assertThat(operator.getOperatorPort().getOpposites(), hasItems(upstream.getOperatorPort()));
        assertThat(operator.getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getInfo(), is(nullValue()));
    }

    /**
     * test for copy.
     */
    @Test
    public void copy() {
        ExternalOutput operator = ExternalOutput.newInstance("out", typeOf(String.class));
        ExternalOutput copy = operator.copy();
        assertThat(copy.toString(), copy.getOperatorKind(), is(OperatorKind.OUTPUT));
        assertThat(copy.getPortKind(), is(PortKind.OUTPUT));
        assertThat(copy.getName(), is("out"));
        assertThat(copy.getOperatorPort().getName(), is(ExternalOutput.PORT_NAME));
        assertThat(copy.getOperatorPort().getOwner(), is(sameInstance((Object) copy)));
        assertThat(copy.getDataType(), is((Object) typeOf(String.class)));
        assertThat(operator.getInfo(), is(nullValue()));
    }
}
