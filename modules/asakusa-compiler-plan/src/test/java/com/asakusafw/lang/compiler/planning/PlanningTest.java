package com.asakusafw.lang.compiler.planning;

import static com.asakusafw.lang.compiler.model.graph.Operators.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.Operators;

/**
 * Test for {@link Planning}.
 * TODO next phase
 */
public class PlanningTest {

    /**
     * normalize - insert begin/end markers.
     */
    @Test
    public void normalize_markers() {
        MockOperators mock = new MockOperators()
            .input("in")
            .operator("a", "i0,i1", "o0,o1").connect("in.*", "a.i0")
            .output("out").connect("a.o0", "out.*");
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        assertThat(g.getOperators(), hasSize(7));
        assertThat(only(PlanMarker.BEGIN, g.getOperators()), hasSize(2));
        assertThat(only(PlanMarker.END, g.getOperators()), hasSize(2));

        mock = new MockOperators(g.rebuild().getOperators());
        assertThat(getPredecessors(mock.get("in")), everyItem(hasMarker(PlanMarker.BEGIN)));
        assertThat(getPredecessors(set(mock.getInput("a.i0"))), not(contains(hasMarker(PlanMarker.BEGIN))));
        assertThat(getPredecessors(set(mock.getInput("a.i1"))), everyItem(hasMarker(PlanMarker.BEGIN)));

        assertThat(getSuccessors(mock.get("out")), everyItem(hasMarker(PlanMarker.END)));
        assertThat(getSuccessors(set(mock.getOutput("a.o0"))), not(contains(hasMarker(PlanMarker.END))));
        assertThat(getSuccessors(set(mock.getOutput("a.o1"))), everyItem(hasMarker(PlanMarker.END)));

        assertThat(mock.get("in"), hasConstraint(OperatorConstraint.GENERATOR));
        assertThat(mock.get("in"), not(hasConstraint(OperatorConstraint.AT_LEAST_ONCE)));
        assertThat(mock.get("out"), not(hasConstraint(OperatorConstraint.GENERATOR)));
        assertThat(mock.get("out"), hasConstraint(OperatorConstraint.AT_LEAST_ONCE));
    }

    /**
     * normalize - flatten.
     */
    @Test
    public void normalize_flatten() {
        MockOperators mock = new MockOperators()
            .input("in1")
            .input("in2")
            .flow("f", new MockOperators()
                .input("fin")
                .operator("a").connect("fin.*", "a.*")
                .operator("b").connect("fin.*", "b.*")
                .output("fout").connect("a.*", "fout.*").connect("b.*", "fout.*")
                .toGraph())
            .connect("in1.*", "f.*").connect("in2.*", "f.*")
            .output("out1").connect("f.*", "out1.*")
            .output("out2").connect("f.*", "out2.*");
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        assertThat(g.getOperators(), hasSize(10));
        assertThat(only(PlanMarker.BEGIN, g.getOperators()), hasSize(2));
        assertThat(only(PlanMarker.END, g.getOperators()), hasSize(2));

        mock = new MockOperators(g.rebuild().getOperators());
        mock.assertConnected("in1.*", "a.*")
            .assertConnected("in1.*", "b.*")
            .assertConnected("in2.*", "a.*")
            .assertConnected("in2.*", "b.*")
            .assertConnected("a.*", "out1.*")
            .assertConnected("a.*", "out2.*")
            .assertConnected("b.*", "out1.*")
            .assertConnected("b.*", "out2.*");
    }

    /**
     * normalize - flatten.
     */
    @Test
    public void normalize_flatten_deep() {
        MockOperators mock = new MockOperators()
            .input("in1")
            .input("in2")
            .flow("f", new MockOperators()
                .input("fin")
                .flow("g", new MockOperators()
                        .input("gin")
                        .operator("a").connect("gin.*", "a.*")
                        .operator("b").connect("gin.*", "b.*")
                        .output("gout").connect("a.*", "gout.*").connect("b.*", "gout.*")
                        .toGraph())
                .connect("fin.*", "g.*")
                .output("fout").connect("g.*", "fout.*")
                .toGraph())
            .connect("in1.*", "f.*").connect("in2.*", "f.*")
            .output("out1").connect("f.*", "out1.*")
            .output("out2").connect("f.*", "out2.*");
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        assertThat(g.getOperators(), hasSize(10));
        assertThat(only(PlanMarker.BEGIN, g.getOperators()), hasSize(2));
        assertThat(only(PlanMarker.END, g.getOperators()), hasSize(2));

        mock = new MockOperators(g.rebuild().getOperators());
        mock.assertConnected("in1.*", "a.*")
            .assertConnected("in1.*", "b.*")
            .assertConnected("in2.*", "a.*")
            .assertConnected("in2.*", "b.*")
            .assertConnected("a.*", "out1.*")
            .assertConnected("a.*", "out2.*")
            .assertConnected("b.*", "out1.*")
            .assertConnected("b.*", "out2.*");
    }

    /**
     * dead-flow elimination - simple.
     */
    @Test
    public void removeDeadFlow_simple() {
        MockOperators mock = new MockOperators()
            .input("in")
            .operator("a").connect("in.*", "a.*")
            .output("out").connect("a.*", "out.*");
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        Planning.removeDeadFlow(g);
        validateDeadFlowElimination(g);
        assertThat(g, hasOperators("in", "a", "out"));
    }

    /**
     * dead-flow elimination - redundant upstreams.
     */
    @Test
    public void removeDeadFlow_redundant_upstreams() {
        MockOperators mock = new MockOperators()
            .input("in")
            .operator("a").connect("in.*", "a.*")
            .output("out").connect("a.*", "out.*")
            .operator("d").connect("d.*", "a.*")
            .operator("dung").connect("dung.*", "d.*");
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        Planning.removeDeadFlow(g);
        validateDeadFlowElimination(g);
        assertThat(g, hasOperators("in", "a", "out"));
    }

    /**
     * dead-flow elimination - redundant downstreams.
     */
    @Test
    public void removeDeadFlow_redundant_downstreams() {
        MockOperators mock = new MockOperators()
            .input("in")
            .operator("a").connect("in.*", "a.*")
            .output("out").connect("a.*", "out.*")
            .operator("d").connect("a.*", "d.*")
            .operator("dung").connect("d.*", "dung.*");
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        Planning.removeDeadFlow(g);
        validateDeadFlowElimination(g);
        assertThat(g, hasOperators("in", "a", "out"));
    }

    /**
     * simplify terminators - simple.
     */
    @Test
    public void simplifyTerminators_simple() {
        MockOperators mock = new MockOperators()
            .input("in")
            .operator("a").connect("in.*", "a.*")
            .output("out").connect("a.*", "out.*");
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        Planning.simplifyTerminators(g);
        validateSimplifyTerminators(g);
    }

    /**
     * simplify terminators - reducing redundant terminators.
     */
    @Test
    public void simplifyTerminators_reduce_redundant_terminators() {
        MockOperators mock = new MockOperators()
            .input("in")
            .operator("a", "in,open", "out,open").connect("in.*", "a.in")
            .output("out").connect("a.out", "out.*");
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        Planning.simplifyTerminators(g);
        validateSimplifyTerminators(g);
    }

    /**
     * simplify terminators - reducing duplicate terminators.
     */
    @Test
    public void simplifyTerminators_reduce_duplicate_terminators() {
        MockOperators mock = new MockOperators()
            .input("in")
            .operator("a").connect("in.*", "a.*")
            .output("out").connect("a.*", "out.*");
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        mock = new MockOperators(g.rebuild().getOperators())
            .marker("dup_begin", PlanMarker.BEGIN)
            .marker("dup_end", PlanMarker.END)
            .connect("dup_begin.*", "in.*")
            .connect("out.*", "dup_end.*");

        Planning.simplifyTerminators(g);
        validateSimplifyTerminators(g);
    }

    @SafeVarargs
    private static <T> Set<T> set(T... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    private void validateDeadFlowElimination(OperatorGraph g) {
        g.rebuild();
        for (Operator operator : g.getOperators()) {
            PlanMarker marker = PlanMarkers.get(operator);

            // each BEGIN must NOT have any predecessors
            // each input port of operator except BEGIN has at least one opposites
            if (marker == PlanMarker.BEGIN) {
                assertThat(Operators.getPredecessors(operator), hasSize(0));
            } else  {
                for (OperatorInput port : operator.getInputs()) {
                    assertThat(port.getOpposites(), hasSize(greaterThanOrEqualTo(1)));
                }
            }

            // each END must NOT have any successors
            // each output port of operator except END has at least one opposites
            if (marker == PlanMarker.END) {
                assertThat(Operators.getSuccessors(operator), hasSize(0));
            } else {
                for (OperatorOutput port : operator.getOutputs()) {
                    assertThat(port.getOpposites(), hasSize(greaterThanOrEqualTo(1)));
                }
            }

            // except BEGIN, operator w/ generator constraint
            // each operator must be backward reachable to any operators with generator constraint
            if (marker != PlanMarker.BEGIN
                    && operator.getConstraints().contains(OperatorConstraint.GENERATOR) == false) {
                Set<Operator> reachables =  Operators.findNearestReachablePredecessors(
                        operator.getInputs(), only(OperatorConstraint.GENERATOR));
                assertThat(reachables, is(not(empty())));
            }

            // except END, operator w/ at-least-once constraint
            // each operator must be forward reachable to any operators with at-least-once constraint
            if (marker != PlanMarker.END
                    && operator.getConstraints().contains(OperatorConstraint.AT_LEAST_ONCE) == false) {
                Set<Operator> reachables =  Operators.findNearestReachableSuccessors(
                        operator.getOutputs(), only(OperatorConstraint.AT_LEAST_ONCE));
                assertThat(reachables, is(not(empty())));
            }
        }
    }


    private void validateSimplifyTerminators(OperatorGraph g) {
        g.rebuild();
        for (Operator operator : g.getOperators()) {
            PlanMarker marker = PlanMarkers.get(operator);
            Set<Operator> succs = Operators.getSuccessors(operator);
            Set<Operator> preds = Operators.getPredecessors(operator);

            // each BEGIN must NOT have any predecessors
            // each operator except BEGIN has at least one predecessors
            if (marker == PlanMarker.BEGIN) {
                assertThat(preds, hasSize(0));
            } else  {
                assertThat(preds, hasSize(greaterThanOrEqualTo(1)));
            }

            // each END must NOT have any successors
            // each operator except END has at least one successors
            if (marker == PlanMarker.END) {
                assertThat(succs, hasSize(0));
            } else {
                assertThat(succs, hasSize(greaterThanOrEqualTo(1)));
            }

            // each operator has upto one BEGIN in the predecessors
            assertThat(only(PlanMarker.BEGIN, preds), hasSize(lessThanOrEqualTo(1)));

            // each operator has upto one END in the successors
            assertThat(only(PlanMarker.END, succs), hasSize(lessThanOrEqualTo(1)));

            // each operator which has BEGIN in its predecessor, it has no other predecessors
            if (only(PlanMarker.BEGIN, preds).isEmpty() == false) {
                assertThat(preds, hasSize(1));
            }

            // each operator which has END in its successors, it has no other successors
            if (only(PlanMarker.END, succs).isEmpty() == false) {
                assertThat(succs, hasSize(1));
            }
        }
    }

    private Matcher<OperatorGraph> hasOperators(String... ids) {
        return new FeatureMatcher<OperatorGraph, Set<String>>(equalTo(set(ids)), "has operators", "operators") {
            @Override
            protected Set<String> featureValueOf(OperatorGraph actual) {
                MockOperators mock = new MockOperators(actual.getOperators());
                Set<String> results = new HashSet<>();
                for (Operator operator : mock.all()) {
                    results.add(mock.id(operator));
                }
                return results;
            }
        };
    }

    private static Matcher<Operator> hasConstraint(OperatorConstraint constraint) {
        return new FeatureMatcher<Operator, Set<OperatorConstraint>>(hasItem(constraint), "has constraint", "constraint") {
            @Override
            protected Set<OperatorConstraint> featureValueOf(Operator actual) {
                return actual.getConstraints();
            }
        };
    }

    private static Matcher<Operator> hasMarker(PlanMarker marker) {
        return new FeatureMatcher<Operator, PlanMarker>(equalTo(marker), "has marker", "marker") {
            @Override
            protected PlanMarker featureValueOf(Operator actual) {
                return PlanMarkers.get(actual);
            }
        };
    }

    private static Operators.Predicate<Operator> only(final OperatorConstraint constraint) {
        return new Operators.Predicate<Operator>() {
            @Override
            public boolean apply(Operator argument) {
                return argument.getConstraints().contains(constraint);
            }
        };
    }

    private static Set<Operator> only(PlanMarker marker, Collection<? extends Operator> operators) {
        Set<Operator> results = new LinkedHashSet<>();
        for (Operator operator : operators) {
            if (PlanMarkers.get(operator) == marker) {
                results.add(operator);
            }
        }
        return results;
    }
}
