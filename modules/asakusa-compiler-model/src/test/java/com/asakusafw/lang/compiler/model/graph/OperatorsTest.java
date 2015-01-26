package com.asakusafw.lang.compiler.model.graph;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;

/**
 * Test for {@link Operators}.
 */
public class OperatorsTest {

    private static final Operators.Predicate<Operator> MARKERS = new Operators.Predicate<Operator>() {
        @Override
        public boolean apply(Operator argument) {
            return argument.getOperatorKind() == OperatorKind.MARKER;
        }
    };

    /**
     * succ.
     */
    @Test
    public void getSuccessors() {
        MockOperators mock = new MockOperators()
            .operator("a", "in", "o0,o1,o2")
            .operator("b", "in", "out")
            .operator("c", "in", "out")
            .operator("d", "in", "out")
            .operator("e", "in", "out")
            .connect("a.o1", "b.in")
            .connect("a.o2", "c.in")
            .connect("a.o2", "d.in")
            .connect("b.out", "e.in");

        assertThat(Operators.getSuccessors(mock.get("a")), is(mock.getAsSet("b", "c", "d")));
    }

    /**
     * pred.
     */
    @Test
    public void getPredecessors() {
        MockOperators mock = new MockOperators()
            .operator("a", "i0,i1,i2", "out")
            .operator("b", "in", "out")
            .operator("c", "in", "out")
            .operator("d", "in", "out")
            .operator("e", "in", "out")
            .connect("b.out", "a.i1")
            .connect("c.out", "a.i2")
            .connect("d.out", "a.i2")
            .connect("e.out", "b.in");

        assertThat(Operators.getPredecessors(mock.get("a")), is(mock.getAsSet("b", "c", "d")));
    }

    /**
     * find nearest.
     */
    @Test
    public void findNearestReachableSuccessors() {
        MockOperators mock = new MockOperators()
            .operator("m0")
            .operator("a").connect("m0.*", "a.*")
            .operator("b").connect("a.*", "b.*")
            .marker("m1").connect("b.*", "m1.*")
            .operator("c").connect("m1.*", "c.*")
            .marker("m2").connect("c.*", "m2.*");

        assertThat(
                Operators.findNearestReachableSuccessors(mock.get("a").getOutputs(), MARKERS),
                is(mock.getAsSet("m1")));
    }

    /**
     * find nearest.
     */
    @Test
    public void findNearestReachableSuccessors_complex() {
        MockOperators mock = new MockOperators()
            .operator("m0")
            .operator("a", "in", "o0,o1").connect("m0.*", "a.*")
            .operator("b").connect("a.o0", "b.*")
            .operator("c").connect("b.*", "c.*")
            .marker("m1").connect("c.*", "m1.*")
            .operator("d").connect("m1.*", "d.*")
            .operator("e").connect("d.*", "e.*").connect("c.*", "e.*")
            .marker("m2").connect("e.*", "m2.*")
            .marker("m3").connect("d.*", "m3.*")
            .marker("m4").connect("d.*", "m4.*").connect("a.o1", "m4.*");

        assertThat(
                Operators.findNearestReachableSuccessors(mock.get("a").getOutputs(), MARKERS),
                is(mock.getAsSet("m1", "m2", "m4")));
    }

    /**
     * find nearest.
     */
    @Test
    public void findNearestReachablePredecessors() {
        MockOperators mock = new MockOperators()
            .operator("m0")
            .operator("a").connect("a.*", "m0.*")
            .operator("b").connect("b.*", "a.*")
            .marker("m1").connect("m1.*", "b.*")
            .operator("c").connect("c.*", "m1.*")
            .marker("m2").connect("m2.*", "c.*");

        assertThat(
                Operators.findNearestReachablePredecessors(mock.get("a").getInputs(), MARKERS),
                is(mock.getAsSet("m1")));
    }

    /**
     * find nearest.
     */
    @Test
    public void findNearestReachablePredecessors_complex() {
        MockOperators mock = new MockOperators()
            .operator("m0")
            .operator("a", "i0,i1", "out").connect("a.*", "m0.*")
            .operator("b").connect("b.*", "a.i0")
            .operator("c").connect("c.*", "b.*")
            .marker("m1").connect("m1.*", "c.*")
            .operator("d").connect("d.*", "m1.*")
            .operator("e").connect("e.*", "d.*").connect("e.*", "c.*")
            .marker("m2").connect("m2.*", "e.*")
            .marker("m3").connect("m3.*", "d.*")
            .marker("m4").connect("m4.*", "d.*").connect("m4.*", "a.i1");

        assertThat(
                Operators.findNearestReachablePredecessors(mock.get("a").getInputs(), MARKERS),
                is(mock.getAsSet("m1", "m2", "m4")));
    }

    /**
     * collect until nearest.
     */
    @Test
    public void collectUntilNearestReachableSuccessors_exclusive() {
        MockOperators mock = new MockOperators()
            .operator("m0")
            .operator("a").connect("m0.*", "a.*")
            .operator("b").connect("a.*", "b.*")
            .marker("m1").connect("b.*", "m1.*")
            .operator("c").connect("m1.*", "c.*")
            .marker("m2").connect("c.*", "m2.*");

        assertThat(
                Operators.collectUntilNearestReachableSuccessors(mock.get("a").getOutputs(), MARKERS, false),
                is(mock.getAsSet("b")));
    }

    /**
     * collect until nearest.
     */
    @Test
    public void collectUntilNearestReachableSuccessors_inclusive() {
        MockOperators mock = new MockOperators()
            .operator("m0")
            .operator("a").connect("m0.*", "a.*")
            .operator("b").connect("a.*", "b.*")
            .marker("m1").connect("b.*", "m1.*")
            .operator("c").connect("m1.*", "c.*")
            .marker("m2").connect("c.*", "m2.*");

        assertThat(
                Operators.collectUntilNearestReachableSuccessors(mock.get("a").getOutputs(), MARKERS, true),
                is(mock.getAsSet("b", "m1")));
    }

    /**
     * collect until nearest.
     */
    @Test
    public void collectUntilNearestReachableSuccessors_complex() {
        MockOperators mock = new MockOperators()
            .operator("m0")
            .operator("a", "in", "o0,o1").connect("m0.*", "a.*")
            .operator("b").connect("a.o0", "b.*")
            .operator("c").connect("b.*", "c.*")
            .marker("m1").connect("c.*", "m1.*")
            .operator("d").connect("m1.*", "d.*")
            .operator("e").connect("d.*", "e.*").connect("c.*", "e.*")
            .marker("m2").connect("e.*", "m2.*")
            .marker("m3").connect("d.*", "m3.*")
            .marker("m4").connect("d.*", "m4.*").connect("a.o1", "m4.*");

        assertThat(
                Operators.collectUntilNearestReachableSuccessors(mock.get("a").getOutputs(), MARKERS, false),
                is(mock.getAsSet("b", "c", "e")));
    }

    /**
     * collect until nearest.
     */
    @Test
    public void collectUntilNearestReachablePredecessors_exclusive() {
        MockOperators mock = new MockOperators()
            .operator("m0")
            .operator("a").connect("a.*", "m0.*")
            .operator("b").connect("b.*", "a.*")
            .marker("m1").connect("m1.*", "b.*")
            .operator("c").connect("c.*", "m1.*")
            .marker("m2").connect("m2.*", "c.*");

        assertThat(
                Operators.collectUntilNearestReachablePredecessors(mock.get("a").getInputs(), MARKERS, false),
                is(mock.getAsSet("b")));
    }

    /**
     * collect until nearest.
     */
    @Test
    public void collectUntilNearestReachablePredecessors_inclusive() {
        MockOperators mock = new MockOperators()
            .operator("m0")
            .operator("a").connect("a.*", "m0.*")
            .operator("b").connect("b.*", "a.*")
            .marker("m1").connect("m1.*", "b.*")
            .operator("c").connect("c.*", "m1.*")
            .marker("m2").connect("m2.*", "c.*");

        assertThat(
                Operators.collectUntilNearestReachablePredecessors(mock.get("a").getInputs(), MARKERS, true),
                is(mock.getAsSet("b", "m1")));
    }

    /**
     * collect until nearest.
     */
    @Test
    public void collectUntilNearestReachablePredecessors_complex() {
        MockOperators mock = new MockOperators()
            .operator("m0")
            .operator("a", "i0,i1", "out").connect("a.*", "m0.*")
            .operator("b").connect("b.*", "a.i0")
            .operator("c").connect("c.*", "b.*")
            .marker("m1").connect("m1.*", "c.*")
            .operator("d").connect("d.*", "m1.*")
            .operator("e").connect("e.*", "d.*").connect("e.*", "c.*")
            .marker("m2").connect("m2.*", "e.*")
            .marker("m3").connect("m3.*", "d.*")
            .marker("m4").connect("m4.*", "d.*").connect("m4.*", "a.i1");

        assertThat(
                Operators.collectUntilNearestReachablePredecessors(mock.get("a").getInputs(), MARKERS, false),
                is(mock.getAsSet("b", "c", "e")));
    }

    /**
     * connectAll.
     */
    @Test
    public void connectAll_many_downstreams() {
        MockOperators mock = new MockOperators()
            .operator("a")
            .operator("b")
            .operator("c");
        Operators.connectAll(
                mock.getOutput("a.*"),
                Arrays.asList(mock.getInput("b.*"), mock.getInput("c.*")));
        mock.assertConnected("a.*", "b.*")
            .assertConnected("a.*", "c.*");
    }

    /**
     * connectAll.
     */
    @Test
    public void connectAll_many_upstreams() {
        MockOperators mock = new MockOperators()
            .operator("a")
            .operator("b")
            .operator("c");
        Operators.connectAll(
                Arrays.asList(mock.getOutput("a.*"), mock.getOutput("b.*")),
                mock.getInput("c.*"));
        mock.assertConnected("a.*", "c.*")
            .assertConnected("b.*", "c.*");
    }

    /**
     * connectAll.
     */
    @Test
    public void connectAll_product() {
        MockOperators mock = new MockOperators()
            .operator("a")
            .operator("b")
            .operator("c")
            .operator("d");
        Operators.connectAll(
                Arrays.asList(mock.getOutput("a.*"), mock.getOutput("b.*")),
                Arrays.asList(mock.getInput("c.*"), mock.getInput("d.*")));
        mock.assertConnected("a.*", "c.*")
            .assertConnected("b.*", "c.*")
            .assertConnected("a.*", "d.*")
            .assertConnected("b.*", "d.*");
    }

    /**
     * insert.
     */
    @Test
    public void insert_output() {
        MockOperators mock = new MockOperators()
            .operator("a")
            .operator("b").connect("a.*", "b.*")
            .operator("c").connect("a.*", "c.*")
            .operator("x");
        Operators.insert(mock.get("x"), mock.getOutput("a.*"));
        mock.assertConnected("a.*", "x.*")
            .assertConnected("a.*", "b.*", false)
            .assertConnected("a.*", "c.*", false)
            .assertConnected("x.*", "b.*")
            .assertConnected("x.*", "c.*");
    }

    /**
     * insert.
     */
    @Test
    public void insert_input() {
        MockOperators mock = new MockOperators()
            .operator("a")
            .operator("b")
            .operator("c").connect("a.*", "c.*").connect("b.*", "c.*")
            .operator("x");
        Operators.insert(mock.get("x"), mock.getInput("c.*"));
        mock.assertConnected("x.*", "c.*")
            .assertConnected("a.*", "c.*", false)
            .assertConnected("b.*", "c.*", false)
            .assertConnected("a.*", "x.*")
            .assertConnected("b.*", "x.*");
    }

    /**
     * insert.
     */
    @Test
    public void insert_connection() {
        MockOperators mock = new MockOperators()
            .operator("a")
            .operator("b")
            .operator("c").connect("a.*", "c.*").connect("b.*", "c.*")
            .operator("d").connect("a.*", "d.*").connect("b.*", "d.*")
            .operator("x");
        Operators.insert(mock.get("x"), mock.getOutput("a.*"), mock.getInput("c.*"));
        mock.assertConnected("a.*", "c.*", false)
            .assertConnected("a.*", "d.*")
            .assertConnected("b.*", "c.*")
            .assertConnected("b.*", "d.*")
            .assertConnected("a.*", "x.*")
            .assertConnected("b.*", "x.*", false)
            .assertConnected("x.*", "c.*")
            .assertConnected("x.*", "d.*", false);
    }

    /**
     * insert.
     */
    @Test
    public void remove() {
        MockOperators mock = new MockOperators()
            .operator("a")
            .operator("b")
            .operator("x").connect("a.*", "x.*").connect("b.*", "x.*")
            .operator("c").connect("x.*", "c.*")
            .operator("d").connect("x.*", "d.*");
        Operators.remove(mock.get("x"));
        mock.assertConnected("a.*", "x.*", false)
            .assertConnected("b.*", "x.*", false)
            .assertConnected("x.*", "c.*", false)
            .assertConnected("x.*", "d.*", false)
            .assertConnected("a.*", "c.*")
            .assertConnected("a.*", "d.*")
            .assertConnected("b.*", "c.*")
            .assertConnected("b.*", "d.*");
    }

    /**
     * replace.
     */
    @Test
    public void replace() {
        MockOperators mock = new MockOperators()
            .operator("a")
            .operator("b")
            .operator("x").connect("a.*", "x.*").connect("b.*", "x.*")
            .operator("c").connect("x.*", "c.*")
            .operator("d").connect("x.*", "d.*")
            .operator("y");
        Operators.replace(mock.get("x"), mock.get("y"));
        mock.assertConnected("a.*", "x.*", false)
            .assertConnected("b.*", "x.*", false)
            .assertConnected("x.*", "c.*", false)
            .assertConnected("x.*", "d.*", false)
            .assertConnected("a.*", "y.*")
            .assertConnected("b.*", "y.*")
            .assertConnected("y.*", "c.*")
            .assertConnected("y.*", "d.*")
            .assertConnected("a.*", "c.*", false)
            .assertConnected("a.*", "d.*", false)
            .assertConnected("b.*", "c.*", false)
            .assertConnected("b.*", "d.*", false);


    }
}
