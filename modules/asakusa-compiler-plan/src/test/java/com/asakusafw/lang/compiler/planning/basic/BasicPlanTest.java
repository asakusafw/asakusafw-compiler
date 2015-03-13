package com.asakusafw.lang.compiler.planning.basic;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.planning.PlanMarker;

/**
 * Test for {@link BasicPlan}.
 */
public class BasicPlanTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end")
            .operator("orphan");

        BasicPlan plan = new BasicPlan();
        BasicSubPlan sub = plan.addElement(mock.getMarkers("begin"), mock.getMarkers("end"));
        for (BasicSubPlan.BasicInput p : sub.getInputs()) {
            assertThat(p.getOwner(), is(sub));
        }
        for (BasicSubPlan.BasicOutput p : sub.getOutputs()) {
            assertThat(p.getOwner(), is(sub));
        }

        assertThat(plan.getElements(), containsInAnyOrder(sub));
        assertThat(sub.getOwner(), is(plan));
        assertThat(sub.getInputs(), hasSize(1));
        assertThat(sub.getOutputs(), hasSize(1));
        assertThat(sub.getOperators(), is(mock.getAsSet("begin", "a", "end")));

        assertThat(sub.findInput(mock.get("begin")), is(notNullValue()));
        assertThat(sub.findOutput(mock.get("begin")), is(nullValue()));

        assertThat(sub.findInput(mock.get("end")), is(nullValue()));
        assertThat(sub.findOutput(mock.get("end")), is(notNullValue()));
    }

    /**
     * connecting test.
     */
    @Test
    public void connect() {
        MockOperators mock = new MockOperators()
            .marker("begin0", PlanMarker.BEGIN)
            .operator("a0").connect("begin0", "a0")
            .marker("end0", PlanMarker.END).connect("a0", "end0")
            .marker("begin1", PlanMarker.BEGIN)
            .operator("a1").connect("begin1", "a1")
            .marker("end1", PlanMarker.END).connect("a1", "end1")
            .marker("begin2", PlanMarker.BEGIN)
            .operator("a2").connect("begin2", "a2")
            .marker("end2", PlanMarker.END).connect("a2", "end2")
            .marker("begin3", PlanMarker.BEGIN)
            .operator("a3").connect("begin3", "a3")
            .marker("end3", PlanMarker.END).connect("a3", "end3");

        BasicPlan plan = new BasicPlan();
        BasicSubPlan s0 = plan.addElement(mock.getMarkers("begin0"), mock.getMarkers("end0"));
        BasicSubPlan s1 = plan.addElement(mock.getMarkers("begin1"), mock.getMarkers("end1"));
        BasicSubPlan s2 = plan.addElement(mock.getMarkers("begin2"), mock.getMarkers("end2"));
        BasicSubPlan s3 = plan.addElement(mock.getMarkers("begin3"), mock.getMarkers("end3"));
        assertThat(plan.getElements(), containsInAnyOrder(s0, s1, s2, s3));

        for (BasicSubPlan s : plan.getElements()) {
            assertThat(s.toString(), s.getOwner(), is(plan));
            for (BasicSubPlan.BasicInput p : s.getInputs()) {
                assertThat(p.toString(), p.getOwner(), is(s));
            }
            for (BasicSubPlan.BasicOutput p : s.getOutputs()) {
                assertThat(p.toString(), p.getOwner(), is(s));
            }
        }

        BasicSubPlan.BasicInput i0 = s0.findInput(mock.get("begin0"));
        BasicSubPlan.BasicInput i1 = s1.findInput(mock.get("begin1"));
        BasicSubPlan.BasicInput i2 = s2.findInput(mock.get("begin2"));
        BasicSubPlan.BasicInput i3 = s3.findInput(mock.get("begin3"));
        BasicSubPlan.BasicOutput o0 = s0.findOutput(mock.get("end0"));
        BasicSubPlan.BasicOutput o1 = s1.findOutput(mock.get("end1"));
        BasicSubPlan.BasicOutput o2 = s2.findOutput(mock.get("end2"));
        BasicSubPlan.BasicOutput o3 = s3.findOutput(mock.get("end3"));

        o0.connect(i1);
        o0.connect(i2);
        o0.connect(i3);
        i3.connect(o0);
        i3.connect(o1);
        i3.connect(o2);
        assertThat(i0.getOpposites(), is(empty()));
        assertThat(i1.getOpposites(), containsInAnyOrder(o0));
        assertThat(i2.getOpposites(), containsInAnyOrder(o0));
        assertThat(i3.getOpposites(), containsInAnyOrder(o0, o1, o2));
        assertThat(o0.getOpposites(), containsInAnyOrder(i1, i2, i3));
        assertThat(o1.getOpposites(), containsInAnyOrder(i3));
        assertThat(o2.getOpposites(), containsInAnyOrder(i3));
        assertThat(o3.getOpposites(), is(empty()));

        o0.disconnect(i3);
        assertThat(i0.getOpposites(), is(empty()));
        assertThat(i1.getOpposites(), containsInAnyOrder(o0));
        assertThat(i2.getOpposites(), containsInAnyOrder(o0));
        assertThat(i3.getOpposites(), containsInAnyOrder(o1, o2));
        assertThat(o0.getOpposites(), containsInAnyOrder(i1, i2));
        assertThat(o1.getOpposites(), containsInAnyOrder(i3));
        assertThat(o2.getOpposites(), containsInAnyOrder(i3));
        assertThat(o3.getOpposites(), is(empty()));
    }

    /**
     * input is not a plan marker.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_input_not_plan_marker() {
        MockOperators mock = new MockOperators()
            .marker("begin")
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        BasicPlan plan = new BasicPlan();
        plan.addElement(mock.getMarkers("begin"), mock.getMarkers("end"));
    }

    /**
     * input w/ preds.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_input_w_preds() {
        MockOperators mock = new MockOperators()
            .operator("invalid")
            .marker("begin", PlanMarker.BEGIN).connect("invalid", "begin")
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        BasicPlan plan = new BasicPlan();
        plan.addElement(mock.getMarkers("begin"), mock.getMarkers("end"));
    }

    /**
     * orphaned input.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_input_orphaned() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .marker("orphan", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        BasicPlan plan = new BasicPlan();
        plan.addElement(mock.getMarkers("begin", "orphan"), mock.getMarkers("end"));
    }

    /**
     * output is not a plan marker.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_output_not_plan_marker() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end").connect("a", "end");

        BasicPlan plan = new BasicPlan();
        plan.addElement(mock.getMarkers("begin"), mock.getMarkers("end"));
    }

    /**
     * output w/ succs.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_output_w_succs() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end")
            .operator("invalid").connect("end", "invalid");

        BasicPlan plan = new BasicPlan();
        plan.addElement(mock.getMarkers("begin"), mock.getMarkers("end"));
    }

    /**
     * orphaned output.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_output_orphaned() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end")
            .marker("orphan", PlanMarker.END);

        BasicPlan plan = new BasicPlan();
        plan.addElement(mock.getMarkers("begin"), mock.getMarkers("end", "orphan"));
    }

    /**
     * body contains orphaned upstream.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_body_orpahned_upstream() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("orphan")
            .operator("a").connect("begin", "a").connect("orphan", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        BasicPlan plan = new BasicPlan();
        plan.addElement(mock.getMarkers("begin"), mock.getMarkers("end"));
    }

    /**
     * body contains orphaned downstream.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_body_orpahned_downstream() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .operator("orphan").connect("a", "orphan")
            .marker("end", PlanMarker.END).connect("a", "end");

        BasicPlan plan = new BasicPlan();
        plan.addElement(mock.getMarkers("begin"), mock.getMarkers("end"));
    }
}
