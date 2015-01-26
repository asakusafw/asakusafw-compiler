package com.asakusafw.lang.compiler.planning;

import static com.asakusafw.lang.compiler.model.graph.Operators.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;

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

        assertThat(mock.get("in"), not(hasConstraint(OperatorConstraint.AT_LEAST_ONCE)));
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

    @SafeVarargs
    private static <T> Set<T> set(T... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
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
