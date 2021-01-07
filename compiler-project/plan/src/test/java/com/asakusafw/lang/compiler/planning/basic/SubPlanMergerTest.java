/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.planning.basic;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanBuilder;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanningTestRoot;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Test for {@link SubPlanMerger}.
 */
public class SubPlanMergerTest extends PlanningTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .operator("o1").connect("b1", "o1")
            .marker("e0", PlanMarker.END).connect("o0", "e0")
            .marker("e1", PlanMarker.END).connect("o1", "e1");

        PlanDetail origin = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("e0"))
            .add(mock.getAsSet("b1"), mock.getAsSet("e1"))
            .build();

        PlanDetail merged = new SubPlanMerger(origin)
            .add(origin.getPlan().getElements())
            .build();

        Plan plan = merged.getPlan();
        assertThat(plan.getElements(), hasSize(1));

        SubPlan s0 = ownerOf(merged, mock.get("b0"));
        assertThat(toOperators(s0.getInputs()), is((Object) toCopies(merged, mock.getAsSet("b0", "b1"))));
        assertThat(toOperators(s0.getOutputs()), is((Object) toCopies(merged, mock.getAsSet("e0", "e1"))));
        assertThat(s0.getOperators(), is((Object) toCopies(merged, mock.all())));
    }

    /**
     * reconnect sub-plans.
     */
    @Test
    public void reconnect() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .marker("c0", PlanMarker.CHECKPOINT).connect("o0", "c0")
            .marker("c1", PlanMarker.CHECKPOINT).connect("o0", "c1")
            .operator("o1").connect("b0", "o0").connect("c0", "o1")
            .operator("o2").connect("b0", "o0").connect("c1", "o2")
            .marker("c2", PlanMarker.CHECKPOINT).connect("o1", "c2")
            .marker("c3", PlanMarker.CHECKPOINT).connect("o2", "c3")
            .operator("o3").connect("c2", "o3").connect("c3", "o3")
            .marker("e0", PlanMarker.END).connect("o3", "e0");

        PlanDetail origin = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("c0", "c1"))
            .add(mock.getAsSet("c0"), mock.getAsSet("c2"))
            .add(mock.getAsSet("c1"), mock.getAsSet("c3"))
            .add(mock.getAsSet("c2", "c3"), mock.getAsSet("e0"))
            .build();

        PlanDetail merged = new SubPlanMerger(origin)
            .add(ownerOf(origin, mock.get("o0")))
            .add(ownerOf(origin, mock.get("o1")), ownerOf(origin, mock.get("o2")))
            .add(ownerOf(origin, mock.get("o3")))
            .build();

        Plan plan = merged.getPlan();
        assertThat(plan.getElements(), hasSize(3));

        SubPlan s0 = ownerOf(merged, mock.get("o0"));
        SubPlan s1 = ownerOf(merged, mock.get("o1"));
        SubPlan s2 = ownerOf(merged, mock.get("o3"));

        assertThat(pred(s0), is(empty()));
        assertThat(succ(s0), containsInAnyOrder(s1));
        assertThat(toOperators(s0.getInputs()), is((Object) toCopies(merged, s0, mock.getAsSet("b0"))));
        assertThat(toOperators(s0.getOutputs()), is((Object) toCopies(merged, s0, mock.getAsSet("c0", "c1"))));
        assertThat(s0.getOperators(), is((Object) toCopies(merged, s0, mock.getAsSet("b0", "o0", "c0", "c1"))));

        assertThat(pred(s1), containsInAnyOrder(s0));
        assertThat(succ(s1), containsInAnyOrder(s2));
        assertThat(toOperators(s1.getInputs()), is((Object) toCopies(merged, s1, mock.getAsSet("c0", "c1"))));
        assertThat(toOperators(s1.getOutputs()), is((Object) toCopies(merged, s1, mock.getAsSet("c2", "c3"))));
        assertThat(s1.getOperators(),
                is((Object) toCopies(merged, s1, mock.getAsSet("c0", "c1", "o1", "o2", "c2", "c3"))));

        assertThat(pred(s2), containsInAnyOrder(s1));
        assertThat(succ(s2), is(empty()));
        assertThat(toOperators(s2.getInputs()), is((Object) toCopies(merged, s2, mock.getAsSet("c2", "c3"))));
        assertThat(toOperators(s2.getOutputs()), is((Object) toCopies(merged, s2, mock.getAsSet("e0"))));
        assertThat(s2.getOperators(), is((Object) toCopies(merged, s2, mock.getAsSet("c2", "c3", "o3", "e0"))));
    }

    /**
     * merge target must have the original owner.
     */
    @Test(expected = IllegalArgumentException.class)
    public void invalid_different_owner() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .operator("o1").connect("b1", "o1")
            .marker("e0", PlanMarker.END).connect("o0", "e0")
            .marker("e1", PlanMarker.END).connect("o1", "e1");

        PlanDetail p0 = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("e0"))
            .build();

        PlanDetail p1 = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b1"), mock.getAsSet("e1"))
            .build();

        List<SubPlan> diff = new ArrayList<>();
        diff.addAll(p0.getPlan().getElements());
        diff.addAll(p1.getPlan().getElements());

        new SubPlanMerger(p0).add(diff);
    }
}
