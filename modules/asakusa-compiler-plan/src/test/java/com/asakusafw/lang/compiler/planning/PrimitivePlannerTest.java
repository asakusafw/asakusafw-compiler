package com.asakusafw.lang.compiler.planning;

import static com.asakusafw.lang.compiler.planning.PlanMarker.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.OperatorGraph;

/**
 * Test for {@link PrimitivePlanner}.
 */
public class PrimitivePlannerTest extends PlanningTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockOperators mock = new MockOperators()
            .input("in")
            .operator("a").connect("in.*", "a.*")
            .output("out").connect("a.*", "out.*");

        PlanDetail detail = plan(mock);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(1));

        SubPlan s = ownerOf(detail, mock.get("a"));
        assertThat(s.getInputs(), hasSize(1));
        assertThat(s.getOutputs(), hasSize(1));
        assertThat(s.getOperators(), hasSize(5));
    }

    /**
     * straight .
     */
    @Test
    public void straight() {
        MockOperators mock = new MockOperators()
            .input("in")
            .operator("o0").connect("in.*", "o0.*")
            .marker("c0", CHECKPOINT).connect("o0.*", "c0.*")
            .operator("o1").connect("c0.*", "o1.*")
            .marker("c1", CHECKPOINT).connect("o1.*", "c1.*")
            .operator("o2").connect("c1.*", "o2.*")
            .output("out").connect("o2.*", "out.*");

        PlanDetail detail = plan(mock);
        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(3));


        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = ownerOf(detail, mock.get("o1"));
        SubPlan s2 = ownerOf(detail, mock.get("o2"));

        assertThat(s0.getInputs(), hasSize(1));
        assertThat(s0.getOutputs(), hasSize(1));
        assertThat(s0.getOperators(), hasSize(4));

        assertThat(s1.getInputs(), hasSize(1));
        assertThat(s1.getOutputs(), hasSize(1));
        assertThat(s1.getOperators(), hasSize(3));

        assertThat(s2.getInputs(), hasSize(1));
        assertThat(s2.getOutputs(), hasSize(1));
        assertThat(s2.getOperators(), hasSize(4));

        assertThat(succ(s0), containsInAnyOrder(s1));
        assertThat(succ(s1), containsInAnyOrder(s2));
        assertThat(succ(s2), is(empty()));

        assertThat(pred(s0), is(empty()));
        assertThat(pred(s1), containsInAnyOrder(s0));
        assertThat(pred(s2), containsInAnyOrder(s1));
    }

    /**
     * minimal inputs.
     */
    @Test
    public void minimal_inputs() {
        MockOperators mock = new MockOperators()
            .marker("b0", BEGIN)
            .marker("b1", BEGIN)
            .operator("o0").connect("b0.*", "o0.*")
            .operator("o1").connect("b1.*", "o1.*")
            .marker("e0", END).connect("o0.*", "e0.*").connect("o1.*", "e0.*");

        PlanDetail detail = plan(mock);
        Plan plan = detail.getPlan();

        assertThat(plan.getElements(), hasSize(2));
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = ownerOf(detail, mock.get("o1"));

        assertThat(pred(s0), is(empty()));
        assertThat(succ(s0), is(empty()));
        assertThat(toOperators(s0.getInputs()), hasOperators("b0"));
        assertThat(toOperators(s0.getOutputs()), hasOperators("e0"));
        assertThat(s0.getOperators(), hasOperators("b0", "o0", "e0"));

        assertThat(pred(s1), is(empty()));
        assertThat(succ(s1), is(empty()));
        assertThat(toOperators(s1.getInputs()), hasOperators("b1"));
        assertThat(toOperators(s1.getOutputs()), hasOperators("e0"));
        assertThat(s1.getOperators(), hasOperators("b1", "o1", "e0"));
    }

    /**
     * minimal outputs.
     */
    @Test
    public void minimal_outputs() {
        MockOperators mock = new MockOperators()
            .marker("b0", BEGIN)
            .operator("o0").connect("b0.*", "o0.*")
            .operator("o1").connect("b0.*", "o1.*")
            .marker("e0", END).connect("o0.*", "e0.*")
            .marker("e1", END).connect("o1.*", "e1.*");

        PlanDetail detail = plan(mock);
        Plan plan = detail.getPlan();

        assertThat(plan.getElements(), hasSize(2));
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = ownerOf(detail, mock.get("o1"));

        assertThat(pred(s0), is(empty()));
        assertThat(succ(s0), is(empty()));
        assertThat(toOperators(s0.getInputs()), hasOperators("b0"));
        assertThat(toOperators(s0.getOutputs()), hasOperators("e0"));
        assertThat(s0.getOperators(), hasOperators("b0", "o0", "e0"));

        assertThat(pred(s1), is(empty()));
        assertThat(succ(s1), is(empty()));
        assertThat(toOperators(s1.getInputs()), hasOperators("b0"));
        assertThat(toOperators(s1.getOutputs()), hasOperators("e1"));
        assertThat(s1.getOperators(), hasOperators("b0", "o1", "e1"));
    }

    /**
     * minimal inputs and outputs.
     */
    @Test
    public void minimal_product() {
        MockOperators mock = new MockOperators()
            .marker("b0", BEGIN)
            .marker("b1", BEGIN)
            .operator("o0").connect("b0.*", "o0.*")
            .operator("o1").connect("b1.*", "o1.*")
            .operator("o2").connect("o0.*", "o2.*").connect("o1.*", "o2.*")
            .operator("o3").connect("o0.*", "o3.*").connect("o1.*", "o3.*")
            .marker("e0", END).connect("o2.*", "e0.*")
            .marker("e1", END).connect("o3.*", "e1.*");

        PlanDetail detail = plan(mock);
        Plan plan = detail.getPlan();

        assertThat(plan.getElements(), hasSize(4));
        SubPlan s0 = ownerOf(detail, mock.getAsSet("b0"), mock.getAsSet("e0"));
        SubPlan s1 = ownerOf(detail, mock.getAsSet("b0"), mock.getAsSet("e1"));
        SubPlan s2 = ownerOf(detail, mock.getAsSet("b1"), mock.getAsSet("e0"));
        SubPlan s3 = ownerOf(detail, mock.getAsSet("b1"), mock.getAsSet("e1"));

        assertThat(pred(s0), is(empty()));
        assertThat(succ(s0), is(empty()));
        assertThat(toOperators(s0.getInputs()), hasOperators("b0"));
        assertThat(toOperators(s0.getOutputs()), hasOperators("e0"));
        assertThat(s0.getOperators(), hasOperators("b0", "e0", "o0", "o2"));

        assertThat(pred(s1), is(empty()));
        assertThat(succ(s1), is(empty()));
        assertThat(toOperators(s1.getInputs()), hasOperators("b0"));
        assertThat(toOperators(s1.getOutputs()), hasOperators("e1"));
        assertThat(s1.getOperators(), hasOperators("b0", "e1", "o0", "o3"));

        assertThat(pred(s2), is(empty()));
        assertThat(succ(s2), is(empty()));
        assertThat(toOperators(s2.getInputs()), hasOperators("b1"));
        assertThat(toOperators(s2.getOutputs()), hasOperators("e0"));
        assertThat(s2.getOperators(), hasOperators("b1", "e0", "o1", "o2"));

        assertThat(pred(s3), is(empty()));
        assertThat(succ(s3), is(empty()));
        assertThat(toOperators(s3.getInputs()), hasOperators("b1"));
        assertThat(toOperators(s3.getOutputs()), hasOperators("e1"));
        assertThat(s3.getOperators(), hasOperators("b1", "e1", "o1", "o3"));
    }

    /**
     * gather.
     */
    @Test
    public void gather() {
        MockOperators mock = new MockOperators()
            .marker("b0", BEGIN)
            .marker("b1", BEGIN)
            .marker("c0", GATHER).connect("b0.*", "c0.*")
            .marker("c1", GATHER).connect("b1.*", "c1.*")
            .operator("o0").connect("c0.*", "o0.*").connect("c1.*", "o0.*")
            .marker("e0", END).connect("o0.*", "e0.*");

        PlanDetail detail = plan(mock);
        Plan plan = detail.getPlan();

        assertThat(plan.getElements(), hasSize(3));
        SubPlan s0 = ownerOf(detail, mock.get("b0"));
        SubPlan s1 = ownerOf(detail, mock.get("b1"));
        SubPlan s2 = ownerOf(detail, mock.get("o0"));

        assertThat(pred(s2), containsInAnyOrder(s0, s1));
        assertThat(succ(s2), is(empty()));
        assertThat(toOperators(s2.getInputs()), hasOperators("c0", "c1"));
        assertThat(toOperators(s2.getOutputs()), hasOperators("e0"));
        assertThat(s2.getOperators(), hasOperators("c0", "c1", "o0", "e0"));
    }

    /**
     * broadcast.
     */
    @Test
    public void broadcast() {
        MockOperators mock = new MockOperators()
            .marker("b0", BEGIN)
            .marker("b1", BEGIN)
            .marker("c0", CHECKPOINT).connect("b0.*", "c0.*")
            .marker("c1", BROADCAST).connect("b1.*", "c1.*")
            .operator("o0").connect("c0.*", "o0.*").connect("c1.*", "o0.*")
            .marker("e0", END).connect("o0.*", "e0.*");

        PlanDetail detail = plan(mock);
        Plan plan = detail.getPlan();

        assertThat(plan.getElements(), hasSize(3));
        SubPlan s0 = ownerOf(detail, mock.get("b0"));
        SubPlan s1 = ownerOf(detail, mock.get("b1"));
        SubPlan s2 = ownerOf(detail, mock.get("o0"));

        assertThat(pred(s2), containsInAnyOrder(s0, s1));
        assertThat(succ(s2), is(empty()));
        assertThat(toOperators(s2.getInputs()), hasOperators("c0", "c1"));
        assertThat(toOperators(s2.getOutputs()), hasOperators("e0"));
        assertThat(s2.getOperators(), hasOperators("c0", "c1", "o0", "e0"));
    }

    /**
     * gather.
     */
    @Test
    public void gather_broadcast() {
        MockOperators mock = new MockOperators()
            .marker("b0", BEGIN)
            .marker("b1", BEGIN)
            .marker("b2", BEGIN)
            .marker("c0", GATHER).connect("b0.*", "c0.*")
            .marker("c1", GATHER).connect("b1.*", "c1.*")
            .marker("c2", BROADCAST).connect("b2.*", "c2.*")
            .operator("o0").connect("c0.*", "o0.*").connect("c1.*", "o0.*").connect("c2.*", "o0.*")
            .marker("e0", END).connect("o0.*", "e0.*");

        PlanDetail detail = plan(mock);
        Plan plan = detail.getPlan();

        assertThat(plan.getElements(), hasSize(4));
        SubPlan s0 = ownerOf(detail, mock.get("b0"));
        SubPlan s1 = ownerOf(detail, mock.get("b1"));
        SubPlan s2 = ownerOf(detail, mock.get("b2"));
        SubPlan s3 = ownerOf(detail, mock.get("o0"));

        assertThat(pred(s3), containsInAnyOrder(s0, s1, s2));
        assertThat(succ(s3), is(empty()));
        assertThat(toOperators(s3.getInputs()), hasOperators("c0", "c1", "c2"));
        assertThat(toOperators(s3.getOutputs()), hasOperators("e0"));
        assertThat(s3.getOperators(), hasOperators("c0", "c1", "c2", "o0", "e0"));
    }

    /**
     * no preds.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_no_predecessors() {
        MockOperators mock = new MockOperators()
            .operator("o0")
            .marker("e0", END).connect("o0.*", "e0.*");
        Planning.createPrimitivePlan(mock.toGraph());
    }

    /**
     * no succs.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_no_successors() {
        MockOperators mock = new MockOperators()
            .marker("b0", BEGIN)
            .operator("o0").connect("b0.*", "o0.*");
        Planning.createPrimitivePlan(mock.toGraph());
    }

    /**
     * GATHER w/ many successors.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_gather_w_many_successors() {
        MockOperators mock = new MockOperators()
            .marker("b0", BEGIN)
            .marker("b1", GATHER).connect("b0.*", "b1.*")
            .operator("o0").connect("b1.*", "o0.*")
            .operator("o1").connect("b1.*", "o1.*")
            .marker("e0", END).connect("o0.*", "e0.*").connect("o1.*", "e0.*");
        Planning.createPrimitivePlan(mock.toGraph());
    }

    /**
     * BROADCAST w/o valid consumers.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_broadcast_wo_consumer() {
        MockOperators mock = new MockOperators()
            .marker("b0", BEGIN)
            .marker("c0", CHECKPOINT).connect("b0.*", "c0.*")
            .marker("c1", BROADCAST).connect("b0.*", "c1.*")
            .operator("o0").connect("c0.*", "o0.*").connect("c1.*", "o0.*")
            .marker("e0", END).connect("o0.*", "e0.*").connect("c1.*", "e0.*");
        Planning.createPrimitivePlan(mock.toGraph());
    }

    private PlanDetail plan(MockOperators mock) {
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        Planning.simplifyTerminators(g);
        PlanDetail detail = Planning.createPrimitivePlan(g);
        validate(detail);
        return detail;
    }

    private static void validate(PlanDetail detail) {
        for (SubPlan sub : detail.getPlan().getElements()) {
            validateInputs(sub.getInputs());
            validateOutputs(sub.getOutputs());
        }
    }

    private static void validateInputs(Set<? extends SubPlan.Input> ports) {
        int nonBroadcast = 0;
        boolean gather = false;
        for (SubPlan.Input port : ports) {
            PlanMarker kind = PlanMarkers.get(port.getOperator());
            assertThat(kind, is(not(nullValue())));
            switch (kind) {
            case BEGIN:
            case END:
            case CHECKPOINT:
                nonBroadcast++;
                break;
            case GATHER:
                nonBroadcast++;
                gather = true;
                break;
            case BROADCAST:
                break;
            default:
                throw new AssertionError();
            }
        }
        if (gather) {
            assertThat(nonBroadcast, is(greaterThanOrEqualTo(1)));
        } else if (gather == false) {
            assertThat(nonBroadcast, is(equalTo(1)));
        }
    }

    private static void validateOutputs(Set<? extends SubPlan.Output> outputs) {
        assertThat(outputs, hasSize(1));
    }
}
