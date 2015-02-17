package com.asakusafw.lang.compiler.analyzer;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.DiagnosticException;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.flow.graph.Connectivity;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;
import com.asakusafw.vocabulary.flow.graph.FlowPartDescription;
import com.asakusafw.vocabulary.flow.graph.InputDescription;
import com.asakusafw.vocabulary.flow.graph.OutputDescription;

/**
 * Test for {@link FlowGraphVerifier}.
 */
public class FlowGraphVerifierTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .operator("o0", Mock.class, "simple")
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();
        FlowGraphVerifier.verify(g);
    }

    /**
     * w/ flow part.
     */
    @Test
    public void nested() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .add("o0", new FlowPartDescription(new MockFlowGraph()
                    .add("s1", new InputDescription("p", String.class))
                    .add("d1", new OutputDescription("p", String.class))
                    .connect("s1", "d1")
                    .toGraph()))
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();
        FlowGraphVerifier.verify(g);
    }

    /**
     * w/ optional open output.
     */
    @Test
    public void optional_open_output() {
        MockFlowGraph mock = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .operator("o0", Mock.class, "simple")
            .add("d0", new OutputDescription("p", String.class))
            .add("open", new InputDescription("open", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0");
        mock.get("open").override(Connectivity.OPTIONAL);
        FlowGraph g = mock.toGraph();
        FlowGraphVerifier.verify(g);
    }

    /**
     * has circuit.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_circuit() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .operator("o0", Mock.class, "simple")
            .operator("o1", Mock.class, "simple")
            .operator("o2", Mock.class, "simple")
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "o1")
            .connect("o1", "o2")
            .connect("o2", "o0")
            .connect("o0", "d0")
            .toGraph();
        FlowGraphVerifier.verify(g);
    }

    /**
     * w/ open input.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_open_input() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .operator("o0", Mock.class, "simple")
            .add("d0", new OutputDescription("p", String.class))
            .add("open", new OutputDescription("open", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();
        FlowGraphVerifier.verify(g);
    }

    /**
     * w/ open output.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_open_output() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .operator("o0", Mock.class, "simple")
            .add("d0", new OutputDescription("p", String.class))
            .add("open", new InputDescription("open", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();
        FlowGraphVerifier.verify(g);
    }

    private static abstract class Mock {

        @MockOperator
        public abstract void simple(String in, Result<String> out);
    }
}
