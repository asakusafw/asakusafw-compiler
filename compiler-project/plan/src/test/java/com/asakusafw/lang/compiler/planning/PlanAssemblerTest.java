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
package com.asakusafw.lang.compiler.planning;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.testing.MockOperators;

/**
 * Test for {@link PlanAssembler}.
 */
public class PlanAssemblerTest extends PlanningTestRoot {

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
        assertThat(origin.getPlan().getElements(), hasSize(2));

        PlanDetail detail = Planning.startAssemblePlan(origin)
            .add(origin.getPlan().getElements())
            .build();
        assertThat(detail.getPlan().getElements(), hasSize(1));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        assertThat(s0.getOperators(), hasOperators("b0", "b1", "o0", "o1", "e0", "e1"));
    }

    /**
     * diamond w/o optimization.
<pre>{@code
b0 +-- o0 --- c0 --- o2 ---- c2 --- o4 --+ e0
    \- o1 --- c1 --- o3 ---- c3 --- o5 -/
}</pre>
     */
    @Test
    public void diamond() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .operator("o1").connect("b0", "o1")
            .marker("c0", PlanMarker.CHECKPOINT).connect("o0", "c0")
            .marker("c1", PlanMarker.CHECKPOINT).connect("o1", "c1")
            .operator("o2").connect("c0", "o2")
            .operator("o3").connect("c1", "o3")
            .marker("c2", PlanMarker.CHECKPOINT).connect("o2", "c2")
            .marker("c3", PlanMarker.CHECKPOINT).connect("o3", "c3")
            .operator("o4").connect("c2", "o4")
            .operator("o5").connect("c3", "o5")
            .marker("e0", PlanMarker.END).connect("o4", "e0").connect("o5", "e0");

        PlanDetail origin = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("c0"))
            .add(mock.getAsSet("b0"), mock.getAsSet("c1"))
            .add(mock.getAsSet("c0"), mock.getAsSet("c2"))
            .add(mock.getAsSet("c1"), mock.getAsSet("c3"))
            .add(mock.getAsSet("c2"), mock.getAsSet("e0"))
            .add(mock.getAsSet("c3"), mock.getAsSet("e0"))
            .build();
        assertThat(origin.getPlan().getElements(), hasSize(6));

        PlanDetail detail = Planning.startAssemblePlan(origin)
            .add(ownersOf(origin, mock.getAsSet("o0", "o1")))
            .add(ownersOf(origin, mock.getAsSet("o2", "o3")))
            .add(ownersOf(origin, mock.getAsSet("o4", "o5")))
            .build();
        assertThat(detail.getPlan().getElements(), hasSize(3));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        SubPlan s1 = ownerOf(detail, mock.get("o2"));
        SubPlan s2 = ownerOf(detail, mock.get("o4"));

        assertThat(pred(s0), is(empty()));
        assertThat(pred(s1), containsInAnyOrder(s0));
        assertThat(pred(s2), containsInAnyOrder(s1));
    }

    /**
     * w/ redundant output elimination.
<pre>{@code
s0 --- o0 +-- d0
           \- d1
}</pre>
     */
    @Test
    public void with_redundant_output_elimination() {
        MockOperators mock = new MockOperators()
            .input("s0")
            .operator("o0").connect("s0", "o0")
            .output("d0").connect("o0", "d0")
            .output("d1").connect("o0", "d1");
        PlanDetail origin = prepare(mock);
        assertThat(origin.getPlan().getElements(), hasSize(2));

        PlanDetail detail = Planning.startAssemblePlan(origin)
            .add(origin.getPlan().getElements())
            .withRedundantOutputElimination(true)
            .build();
        assertThat(detail.getPlan().getElements(), hasSize(1));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        assertThat(s0.getInputs(), hasSize(1));
        assertThat(s0.getOutputs(), hasSize(2));
        assertThat(getDuplications(s0), hasSize(0));
    }

    /**
     * w/ union push down.
<pre>{@code
s0 --+ o0 --- d0
s1 -/
}</pre>
     */
    @Test
    public void with_union_push_down() {
        MockOperators mock = new MockOperators()
            .input("s0")
            .input("s1")
            .operator("o0").connect("s0", "o0").connect("s1", "o0")
            .output("d0").connect("o0", "d0");
        PlanDetail origin = prepare(mock);
        assertThat(origin.getPlan().getElements(), hasSize(2));

        PlanDetail detail = Planning.startAssemblePlan(origin)
            .add(origin.getPlan().getElements())
            .withUnionPushDown(true)
            .build();
        assertThat(detail.getPlan().getElements(), hasSize(1));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        assertThat(s0.getInputs(), hasSize(2));
        assertThat(s0.getOutputs(), hasSize(1));
        assertThat(getDuplications(s0), hasSize(0));
    }

    /**
     * w/ trivial output elimination.
<pre>{@code
s0 --+ o1 --- d0
s1 -/
:: where s1 is not generator
>>>
s0 --- o1 --- d0
}</pre>
     */
    @Test
    public void with_trivial_output_elimination() {
        MockOperators mock = new MockOperators()
            .input("s0")
            .operator("s1")
            .operator("o0").connect("s0", "o0").connect("s1", "o0")
            .output("d0").connect("o0", "d0");
        PlanDetail origin = prepare(mock);
        assertThat(origin.getPlan().getElements(), hasSize(2));

        PlanDetail detail = Planning.startAssemblePlan(origin)
            .add(origin.getPlan().getElements())
            .withTrivialOutputElimination(true)
            .withUnionPushDown(true)
            .build();
        assertThat(detail.getPlan().getElements(), hasSize(1));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        assertThat(s0.getInputs(), hasSize(1));
        assertThat(s0.getOutputs(), hasSize(1));
        assertThat(detail.getCopies(mock.get("s1")), hasSize(0));
        assertThat(getDuplications(s0), hasSize(0));
    }

    /**
     * w/ duplicate checkpoint elimination.
<pre>{@code
s0 --- c0 --- c1 --- d0
}</pre>
     */
    @Test
    public void with_duplicate_checkpoint_elimination() {
        MockOperators mock = new MockOperators()
            .input("s0")
            .marker("c0", PlanMarker.CHECKPOINT).connect("s0", "c0")
            .marker("c1", PlanMarker.CHECKPOINT).connect("c0", "c1")
            .output("d0").connect("c1", "d0");
        PlanDetail origin = prepare(mock);
        assertThat(origin.getPlan().getElements(), hasSize(3));

        PlanAssembler assembler = Planning.startAssemblePlan(origin);
        assembler.withDuplicateCheckpointElimination(true);
        for (SubPlan s : origin.getPlan().getElements()) {
            assembler.add(Collections.singleton(s));
        }
        PlanDetail detail = assembler.build();
        assertThat(detail.getPlan().getElements(), hasSize(2));

        SubPlan s0 = ownerOf(detail, mock.get("c0"));
        SubPlan s1 = ownerOf(detail, mock.get("c1"));
        assertThat(succ(s0), containsInAnyOrder(s1));

        assertThat(getDuplications(s0), hasSize(0));
        assertThat(getDuplications(s1), hasSize(0));
    }

    /**
     * w/ crossing.
<pre>{@code
s0 --+ o0 +-- d0
s1 -/      \- d1
}</pre>
     */
    @Test
    public void complex_cross() {
        MockOperators mock = new MockOperators()
            .input("s0")
            .input("s1")
            .operator("o0").connect("s0", "o0").connect("s1", "o0")
            .output("d0").connect("o0", "d0")
            .output("d1").connect("o0", "d1");
        PlanDetail origin = prepare(mock);
        assertThat(origin.getPlan().getElements(), hasSize(4));

        PlanDetail detail = Planning.startAssemblePlan(origin)
            .withRedundantOutputElimination(true)
            .withUnionPushDown(true)
            .withSortResult(true)
            .add(origin.getPlan().getElements())
            .build();
        assertThat(detail.getPlan().getElements(), hasSize(1));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        assertThat(s0.getInputs(), hasSize(2));
        assertThat(s0.getOutputs(), hasSize(2));
        assertThat(getDuplications(s0), hasSize(0));
    }

    /**
     * w/ gathering.
<pre>{@code
s0 --+ o0 --- g0 --- o1 +-- d0
s1 -/                    \- d1
}</pre>
     */
    @Test
    public void complex_gather() {
        MockOperators mock = new MockOperators()
            .input("s0")
            .input("s1")
            .marker("o0").connect("s0", "o0").connect("s1", "o0")
            .marker("g0", PlanMarker.GATHER).connect("o0", "g0")
            .operator("o1").connect("g0", "o1")
            .output("d0").connect("o1", "d0")
            .output("d1").connect("o1", "d1");
        PlanDetail origin = prepare(mock);
        assertThat(origin.getPlan().getElements(), hasSize(4));

        PlanDetail detail = Planning.startAssemblePlan(origin)
            .withRedundantOutputElimination(true)
            .withUnionPushDown(true)
            .withSortResult(true)
            .add(ownersOf(origin, mock.getAsSet("o0")))
            .add(ownersOf(origin, mock.getAsSet("o1")))
            .build();
        assertThat(detail.getPlan().getElements(), hasSize(2));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        assertThat(s0.getInputs(), hasSize(2));
        assertThat(s0.getOutputs(), hasSize(1));
        assertThat(s0.getOperators(), hasSize(6));
        assertThat(getDuplications(s0), hasSize(0));

        SubPlan s1 = ownerOf(detail, mock.get("o1"));
        assertThat(s1.getInputs(), hasSize(1));
        assertThat(s1.getOutputs(), hasSize(2));
        assertThat(s1.getOperators(), hasSize(6));
        assertThat(getDuplications(s1), hasSize(0));
    }

    /**
     * w/ broadcast.
<pre>{@code
s0 --- o0 --- b0 --+ o1 --- d0
              s1 -/
              s2 -/
}</pre>
     */
    @Test
    public void complex_broadcast() {
        MockOperators mock = new MockOperators()
            .input("s0")
            .operator("o0")
                .connect("s0", "o0")
            .marker("b0", PlanMarker.BROADCAST)
                .connect("o0", "b0")
            .input("s1")
            .input("s2")
            .operator("o1", "i0,i1", "o0")
                .connect("s1", "o1.i0")
                .connect("s2", "o1.i0")
                .connect("b0", "o1.i1")
            .output("d0").connect("o1", "d0");
        PlanDetail origin = prepare(mock);
        assertThat(origin.getPlan().getElements(), hasSize(3));

        PlanDetail detail = Planning.startAssemblePlan(origin)
            .withRedundantOutputElimination(true)
            .withUnionPushDown(true)
            .withSortResult(true)
            .add(ownersOf(origin, mock.getAsSet("o0")))
            .add(ownersOf(origin, mock.getAsSet("o1")))
            .build();
        assertThat(detail.getPlan().getElements(), hasSize(2));

        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        assertThat(s0.getInputs(), hasSize(1));
        assertThat(s0.getOutputs(), hasSize(1));
        assertThat(getDuplications(s0), hasSize(0));

        SubPlan s1 = ownerOf(detail, mock.get("o1"));
        assertThat(s1.getInputs(), hasSize(3));
        assertThat(s1.getOutputs(), hasSize(1));
        assertThat(getDuplications(s1), hasSize(0));
    }

    /**
     * w/ custom equivalence.
<pre>{@code
in0 +-- c0 --- out0
     \- c1 --- out1
     \- c2 --- out2
}</pre>
     */
    @Test
    public void custom_equivalence() {
        MockOperators mock = new MockOperators()
            .input("in0")
            .marker("c0", PlanMarker.CHECKPOINT).connect("in0", "c0")
            .marker("c1", PlanMarker.CHECKPOINT).connect("in0", "c1")
            .marker("c2", PlanMarker.CHECKPOINT).connect("in0", "c2")
            .output("out0").connect("c0", "out0")
            .output("out1").connect("c1", "out1")
            .output("out2").connect("c2", "out2");
        PlanDetail origin = prepare(mock);
        assertThat(origin.getPlan().getElements(), hasSize(6));
        mock = new MockOperators(origin.getSources()); // rebuild mock

        PlanDetail detail = Planning.startAssemblePlan(origin)
            .add(ownersOf(origin, mock.getAsSet("in0")))
            .add(ownersOf(origin, mock.getAsSet("out0")))
            .add(ownersOf(origin, mock.getAsSet("out1")))
            .add(ownersOf(origin, mock.getAsSet("out2")))
            .withRedundantOutputElimination(true)
            .withDuplicateCheckpointElimination(true)
            .withCustomEquivalence((owner, operator) -> {
                if (owner.findOutput(operator) != null) {
                    return PlanMarkers.get(operator);
                }
                return PlanAssembler.DEFAULT_EQUIVALENCE.extract(owner, operator);
            })
            .build();
        assertThat(detail.getPlan().getElements(), hasSize(4));

        SubPlan s0 = ownerOf(detail, mock.get("in0"));
        SubPlan s1 = ownerOf(detail, mock.get("out0"));
        SubPlan s2 = ownerOf(detail, mock.get("out1"));
        SubPlan s3 = ownerOf(detail, mock.get("out2"));

        assertThat(s0.getOutputs(), hasSize(1));
        assertThat(succ(s0), containsInAnyOrder(s1, s2, s3));
    }

    private PlanDetail prepare(MockOperators mock) {
        OperatorGraph g = mock.toGraph();
        Planning.normalize(g);
        Planning.simplifyTerminators(g);
        return Planning.createPrimitivePlan(g);
    }

    private Set<Operator> getDuplications(SubPlan sub) {
        Map<Long, Operator> saw = new HashMap<>();
        Set<Operator> dups = new HashSet<>();
        for (Operator operator : sub.getOperators()) {
            Long id = operator.getSerialNumber();
            if (saw.containsKey(id)) {
                dups.add(saw.get(id));
                dups.add(operator);
            }
            saw.put(id, operator);
        }
        return dups;
    }
}
