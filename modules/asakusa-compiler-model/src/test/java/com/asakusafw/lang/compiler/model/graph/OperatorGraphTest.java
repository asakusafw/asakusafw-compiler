package com.asakusafw.lang.compiler.model.graph;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

/**
 * Test for {@link OperatorGraph}.
 */
public class OperatorGraphTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockOperators operators = new MockOperators()
                .operator("a", "in", "out");

        OperatorGraph graph = new OperatorGraph();
        assertThat(graph.toString(), graph.getOperators(), hasSize(0));

        graph.add(operators.get("a"));
        assertThat(graph.toString(), graph.getOperators(), hasSize(1));
        assertThat(graph.toString(), graph.getOperators(), hasItem(operators.get("a")));

        graph.remove(operators.get("a"));
        assertThat(graph.toString(), graph.getOperators(), hasSize(0));
    }

    /**
     * rebuilding.
     */
    @Test
    public void rebuild() {
        MockOperators operators = new MockOperators()
                .operator("a", "p", "p")
                .operator("b", "p", "p")
                .operator("c", "p", "p")
                .connect("a.p", "b.p")
                .connect("b.p", "c.p");

        OperatorGraph graph = new OperatorGraph();
        graph.add(operators.get("b"));

        assertThat(graph.getOperators(), hasSize(1));
        assertThat(graph.getOperators(), hasItem(operators.get("b")));

        graph.rebuild();
        assertThat(graph.getOperators(), hasSize(3));
        assertThat(graph.getOperators(), hasItem(operators.get("a")));
        assertThat(graph.getOperators(), hasItem(operators.get("b")));
        assertThat(graph.getOperators(), hasItem(operators.get("c")));
    }

    /**
     * copy.
     */
    @Test
    public void copy() {
        MockOperators operators = new MockOperators()
                .operator("a", "p", "p")
                .operator("b", "p", "p")
                .operator("c", "p", "p")
                .operator("d", "p", "p")
                .connect("a.p", "b.p")
                .connect("b.p", "c.p")
                .connect("c.p", "d.p");

        OperatorGraph graph = new OperatorGraph();
        graph.add(operators.get("b"));
        graph.add(operators.get("c"));

        MockOperators copy = new MockOperators(graph.copy().rebuild().getOperators());
        assertThat(OperatorGraph.getAllOperators(copy.all()), hasSize(4));
        copy.assertConnected("a.p", "b.p");
        copy.assertConnected("b.p", "c.p");
        copy.assertConnected("c.p", "d.p");
    }

    /**
     * range copy.
     */
    @Test
    public void copy_range() {
        MockOperators operators = new MockOperators()
                .operator("a", "p", "p")
                .operator("b", "p", "p")
                .operator("c", "p", "p")
                .operator("d", "p", "p")
                .connect("a.p", "b.p")
                .connect("b.p", "c.p")
                .connect("c.p", "d.p");

        OperatorGraph graph = new OperatorGraph();
        graph.add(operators.get("b"));
        graph.add(operators.get("c"));

        MockOperators copy = new MockOperators(
                OperatorGraph.getAllOperators(
                        OperatorGraph.copy(graph.getOperators()).values()));
        assertThat(OperatorGraph.getAllOperators(copy.all()), hasSize(2));
        copy.assertConnected("b.p", "c.p");
    }

    /**
     * find inputs.
     */
    @Test
    public void inputs() {
        MockOperators operators = new MockOperators()
                .input("i0")
                .operator("u0", "in", "out")
                .output("o0")
                .input("i1")
                .operator("u1", "in", "out")
                .output("o1");

        OperatorGraph graph = new OperatorGraph(operators.all());
        Map<String, ExternalInput> inputs = graph.getInputs();
        assertThat(inputs.keySet(), hasSize(2));
        assertThat(inputs, hasEntry("i0", operators.get("i0")));
        assertThat(inputs, hasEntry("i1", operators.get("i1")));
    }

    /**
     * find outputs.
     */
    @Test
    public void outputs() {
        MockOperators operators = new MockOperators()
                .input("i0")
                .operator("u0", "in", "out")
                .output("o0")
                .input("i1")
                .operator("u1", "in", "out")
                .output("o1");

        OperatorGraph graph = new OperatorGraph(operators.all());
        Map<String, ExternalOutput> outputs = graph.getOutputs();
        assertThat(outputs.keySet(), hasSize(2));
        assertThat(outputs, hasEntry("o0", operators.get("o0")));
        assertThat(outputs, hasEntry("o1", operators.get("o1")));
    }

    /**
     * find outputs.
     */
    @Test
    public void clear() {
        MockOperators operators = new MockOperators()
                .input("i0")
                .operator("u0", "in", "out")
                .output("o0")
                .input("i1")
                .operator("u1", "in", "out")
                .output("o1");

        OperatorGraph graph = new OperatorGraph(operators.all());
        assertThat(graph.getOperators(), hasSize(greaterThan(0)));

        graph.clear();
        assertThat(graph.getOperators(), hasSize(0));
    }
}
