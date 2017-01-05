/**
 * Copyright 2011-2017 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.lang.compiler.planning;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.testing.MockOperators;

/**
 * Test for {@link PlanBuilder}.
 */
public class PlanBuilderTest extends PlanningTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("end"))
            .build();

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(1));

        SubPlan s = plan.getElements().iterator().next();
        assertThat(s.getInputs(), hasSize(1));
        assertThat(s.getOutputs(), hasSize(1));
        assertThat(toSources(detail, s.getOperators()), is(mock.all()));
    }

    /**
     * multiple plans.
     */
    @Test
    public void multiple() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("cp", PlanMarker.CHECKPOINT).connect("a", "cp")
            .operator("b").connect("cp", "b")
            .marker("end", PlanMarker.END).connect("b", "end");

        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("cp"))
            .add(mock.getAsSet("cp"), mock.getAsSet("end"))
            .build();

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(2));

        SubPlan s0 = ownerOf(detail, mock.get("a"));
        SubPlan s1 = ownerOf(detail, mock.get("b"));

        SubPlan.Input i0 = s0.findInput(copyOf(detail, s0, mock.get("begin")));
        SubPlan.Input i1 = s1.findInput(copyOf(detail, s1, mock.get("cp")));
        SubPlan.Output o0 = s0.findOutput(copyOf(detail, s0, mock.get("cp")));
        SubPlan.Output o1 = s1.findOutput(copyOf(detail, s1, mock.get("end")));

        assertThat(i0.getOpposites(), is(empty()));
        assertThat(i1.getOpposites(), containsInAnyOrder(o0));
        assertThat(o0.getOpposites(), containsInAnyOrder(i1));
        assertThat(o1.getOpposites(), is(empty()));
    }

    /**
     * diamond plans w/ difference checkpoints.
     */
    @Test
    public void diamond_diff_checkpoints() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("o0")
            .operator("o1")
            .operator("o2")
            .operator("o3")
            .marker("cp0", PlanMarker.CHECKPOINT)
            .marker("cp1", PlanMarker.CHECKPOINT)
            .marker("cp2", PlanMarker.CHECKPOINT)
            .marker("cp3", PlanMarker.CHECKPOINT)
            .marker("end", PlanMarker.END)
            .connect("begin", "o0")
            .connect("o0", "cp0")
            .connect("o0", "cp1")
            .connect("cp0", "o1")
            .connect("cp1", "o2")
            .connect("o1", "cp2")
            .connect("o2", "cp3")
            .connect("cp2", "o3")
            .connect("cp3", "o3")
            .connect("o3", "end");

        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("cp0", "cp1"))
            .add(mock.getAsSet("cp0"), mock.getAsSet("cp2"))
            .add(mock.getAsSet("cp1"), mock.getAsSet("cp3"))
            .add(mock.getAsSet("cp2", "cp3"), mock.getAsSet("end"))
            .build();

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = ownerOf(detail, mock.get("o1"));
        SubPlan s2 = ownerOf(detail, mock.get("o2"));
        SubPlan s3 = ownerOf(detail, mock.get("o3"));

        SubPlan.Input i0 = s0.findInput(copyOf(detail, s0, mock.get("begin")));
        SubPlan.Input i1 = s1.findInput(copyOf(detail, s1, mock.get("cp0")));
        SubPlan.Input i2 = s2.findInput(copyOf(detail, s2, mock.get("cp1")));
        SubPlan.Input i3a = s3.findInput(copyOf(detail, s3, mock.get("cp2")));
        SubPlan.Input i3b = s3.findInput(copyOf(detail, s3, mock.get("cp3")));
        SubPlan.Output o0a = s0.findOutput(copyOf(detail, s0, mock.get("cp0")));
        SubPlan.Output o0b = s0.findOutput(copyOf(detail, s0, mock.get("cp1")));
        SubPlan.Output o1 = s1.findOutput(copyOf(detail, s1, mock.get("cp2")));
        SubPlan.Output o2 = s2.findOutput(copyOf(detail, s2, mock.get("cp3")));
        SubPlan.Output o3 = s3.findOutput(copyOf(detail, s3, mock.get("end")));

        assertThat(i0.getOpposites(), is(empty()));
        assertThat(i1.getOpposites(), containsInAnyOrder(o0a));
        assertThat(i2.getOpposites(), containsInAnyOrder(o0b));
        assertThat(i3a.getOpposites(), containsInAnyOrder(o1));
        assertThat(i3b.getOpposites(), containsInAnyOrder(o2));
        assertThat(o0a.getOpposites(), containsInAnyOrder(i1));
        assertThat(o0b.getOpposites(), containsInAnyOrder(i2));
        assertThat(o1.getOpposites(), containsInAnyOrder(i3a));
        assertThat(o2.getOpposites(), containsInAnyOrder(i3b));
        assertThat(o3.getOpposites(), is(empty()));
    }

    /**
     * diamond plans w/ sharing input checkpoints.
     */
    @Test
    public void diamond_shared_input() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("o0")
            .operator("o1")
            .operator("o2")
            .operator("o3")
            .marker("cp0", PlanMarker.CHECKPOINT)
            .marker("cp1", PlanMarker.CHECKPOINT)
            .marker("cp2", PlanMarker.CHECKPOINT)
            .marker("end", PlanMarker.END)
            .connect("begin", "o0")
            .connect("o0", "cp0")
            .connect("cp0", "o1")
            .connect("cp0", "o2")
            .connect("o1", "cp1")
            .connect("o2", "cp2")
            .connect("cp1", "o3")
            .connect("cp2", "o3")
            .connect("o3", "end");

        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("cp0"))
            .add(mock.getAsSet("cp0"), mock.getAsSet("cp1"))
            .add(mock.getAsSet("cp0"), mock.getAsSet("cp2"))
            .add(mock.getAsSet("cp1", "cp2"), mock.getAsSet("end"))
            .build();

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = ownerOf(detail, mock.get("o1"));
        SubPlan s2 = ownerOf(detail, mock.get("o2"));
        SubPlan s3 = ownerOf(detail, mock.get("o3"));

        SubPlan.Input i0 = s0.findInput(copyOf(detail, s0, mock.get("begin")));
        SubPlan.Input i1 = s1.findInput(copyOf(detail, s1, mock.get("cp0")));
        SubPlan.Input i2 = s2.findInput(copyOf(detail, s2, mock.get("cp0")));
        SubPlan.Input i3a = s3.findInput(copyOf(detail, s3, mock.get("cp1")));
        SubPlan.Input i3b = s3.findInput(copyOf(detail, s3, mock.get("cp2")));
        SubPlan.Output o0 = s0.findOutput(copyOf(detail, s0, mock.get("cp0")));
        SubPlan.Output o1 = s1.findOutput(copyOf(detail, s1, mock.get("cp1")));
        SubPlan.Output o2 = s2.findOutput(copyOf(detail, s2, mock.get("cp2")));
        SubPlan.Output o3 = s3.findOutput(copyOf(detail, s3, mock.get("end")));

        assertThat(i0.getOpposites(), is(empty()));
        assertThat(i1.getOpposites(), containsInAnyOrder(o0));
        assertThat(i2.getOpposites(), containsInAnyOrder(o0));
        assertThat(i3a.getOpposites(), containsInAnyOrder(o1));
        assertThat(i3b.getOpposites(), containsInAnyOrder(o2));
        assertThat(o0.getOpposites(), containsInAnyOrder(i1, i2));
        assertThat(o1.getOpposites(), containsInAnyOrder(i3a));
        assertThat(o2.getOpposites(), containsInAnyOrder(i3b));
        assertThat(o3.getOpposites(), is(empty()));
    }

    /**
     * diamond plans w/ shared outputs.
     */
    @Test
    public void diamond_shared_output() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("o0")
            .operator("o1")
            .operator("o2")
            .operator("o3")
            .marker("cp0", PlanMarker.CHECKPOINT)
            .marker("cp1", PlanMarker.CHECKPOINT)
            .marker("cp2", PlanMarker.CHECKPOINT)
            .marker("end", PlanMarker.END)
            .connect("begin", "o0")
            .connect("o0", "cp0")
            .connect("o0", "cp1")
            .connect("cp0", "o1")
            .connect("cp1", "o2")
            .connect("o1", "cp2")
            .connect("o2", "cp2")
            .connect("cp2", "o3")
            .connect("o3", "end");

        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("cp0", "cp1"))
            .add(mock.getAsSet("cp0"), mock.getAsSet("cp2"))
            .add(mock.getAsSet("cp1"), mock.getAsSet("cp2"))
            .add(mock.getAsSet("cp2"), mock.getAsSet("end"))
            .build();

        Plan plan = detail.getPlan();
        assertThat(plan.getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = ownerOf(detail, mock.get("o1"));
        SubPlan s2 = ownerOf(detail, mock.get("o2"));
        SubPlan s3 = ownerOf(detail, mock.get("o3"));

        SubPlan.Input i0 = s0.findInput(copyOf(detail, s0, mock.get("begin")));
        SubPlan.Input i1 = s1.findInput(copyOf(detail, s1, mock.get("cp0")));
        SubPlan.Input i2 = s2.findInput(copyOf(detail, s2, mock.get("cp1")));
        SubPlan.Input i3 = s3.findInput(copyOf(detail, s3, mock.get("cp2")));
        SubPlan.Output o0a = s0.findOutput(copyOf(detail, s0, mock.get("cp0")));
        SubPlan.Output o0b = s0.findOutput(copyOf(detail, s0, mock.get("cp1")));
        SubPlan.Output o1 = s1.findOutput(copyOf(detail, s1, mock.get("cp2")));
        SubPlan.Output o2 = s2.findOutput(copyOf(detail, s2, mock.get("cp2")));
        SubPlan.Output o3 = s3.findOutput(copyOf(detail, s3, mock.get("end")));

        assertThat(i0.getOpposites(), is(empty()));
        assertThat(i1.getOpposites(), containsInAnyOrder(o0a));
        assertThat(i2.getOpposites(), containsInAnyOrder(o0b));
        assertThat(i3.getOpposites(), containsInAnyOrder(o1, o2));
        assertThat(o0a.getOpposites(), containsInAnyOrder(i1));
        assertThat(o0b.getOpposites(), containsInAnyOrder(i2));
        assertThat(o1.getOpposites(), containsInAnyOrder(i3));
        assertThat(o2.getOpposites(), containsInAnyOrder(i3));
        assertThat(o3.getOpposites(), is(empty()));
    }

    /**
     * input is empty.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_empty_input() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        PlanBuilder.from(mock.all())
            .add(Collections.emptySet(), mock.getAsSet("end"))
            .build();
    }

    /**
     * output is empty.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_empty_output() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), Collections.emptySet())
            .build();
    }

    /**
     * common inputs/outputs.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_common_io() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin", "end"), mock.getAsSet("end", "begin"))
            .build();
    }

    /**
     * input is not a plan marker.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_input_not_plan_marker() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin", "a"), mock.getAsSet("end"))
            .build();
    }

    /**
     * input is not from a source.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_input_not_source() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        MockOperators other = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        PlanBuilder.from(mock.all())
            .add(other.getAsSet("begin"), mock.getAsSet("end"))
            .build();
    }

    /**
     * input is orphaned.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_input_orphaned() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end")
            .marker("orphan", PlanMarker.CHECKPOINT);

        PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin", "orphan"), mock.getAsSet("end"))
            .build();
    }

    /**
     * input reaches other inputs.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_input_reach_input_strict() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end")
            .marker("invalid", PlanMarker.CHECKPOINT).connect("invalid", "begin").connect("invalid", "a");

        PlanBuilder.from(mock.all())
            .withStrict(true)
            .add(mock.getAsSet("begin", "invalid"), mock.getAsSet("end"))
            .build();
    }

    /**
     * output is not a plan marker.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_output_not_plan_marker() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("end", "a"))
            .build();
    }

    /**
     * output is not in sources.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_output_not_source() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        MockOperators other = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), other.getAsSet("end"))
            .build();
    }

    /**
     * output is orphaned.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_output_orphaned() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end")
            .marker("orphan", PlanMarker.CHECKPOINT);

        PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("end", "orphan"))
            .build();
    }

    /**
     * output reaches other outputs.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_output_reach_output_strict() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end")
            .marker("invalid", PlanMarker.CHECKPOINT).connect("end", "invalid").connect("a", "invalid");

        PlanBuilder.from(mock.all())
            .withStrict(true)
            .add(mock.getAsSet("begin"), mock.getAsSet("end", "invalid"))
            .build();
    }
}
