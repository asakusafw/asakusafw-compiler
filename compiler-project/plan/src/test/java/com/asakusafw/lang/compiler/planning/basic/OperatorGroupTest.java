/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import java.util.BitSet;
import java.util.Set;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.planning.PlanBuilder;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanningTestRoot;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.compiler.planning.basic.OperatorGroup.Attribute;

/**
 * Test for {@link OperatorGroup}.
 */
public class OperatorGroupTest extends PlanningTestRoot {

    /**
     * attributes - extract.
     */
    @Test
    public void attributes_extract() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        Set<Attribute> attrs = OperatorGroup.getAttributes(s0, copyOf(detail, mock.get("o0")));
        assertThat(attrs, contains(Attribute.EXTRACT_KIND));
    }

    /**
     * attributes - extract.
     */
    @Test
    public void attributes_non_extract() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("g0", PlanMarker.GATHER).connect("b0", "g0")
            .operator("o0").connect("g0", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("g0"))
            .add(mock.getAsSet("g0"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("b0"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));
        Set<Attribute> a0 = OperatorGroup.getAttributes(s0, copyOf(detail, s0, mock.get("g0")));
        Set<Attribute> a1 = OperatorGroup.getAttributes(s1, copyOf(detail, s1, mock.get("g0")));
        Set<Attribute> a2 = OperatorGroup.getAttributes(s1, copyOf(detail, s1, mock.get("o0")));
        assertThat(a0, hasItem(Attribute.GATHER));
        assertThat(a1, hasItem(Attribute.GATHER));
        assertThat(a0, hasItem(Attribute.EXTRACT_KIND));
        assertThat(a1, not(hasItem(Attribute.EXTRACT_KIND)));
        assertThat(a2, not(hasItem(Attribute.EXTRACT_KIND)));
    }

    /**
     * attributes - generator.
     */
    @Test
    public void attributes_generator() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .operator("o0", OperatorConstraint.GENERATOR).connect("b0", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        Set<Attribute> attrs = OperatorGroup.getAttributes(s0, copyOf(detail, mock.get("o0")));
        assertThat(attrs, hasItem(Attribute.GENERATOR));
    }

    /**
     * attributes - consumer.
     */
    @Test
    public void attributes_consumer() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .operator("o0", OperatorConstraint.AT_LEAST_ONCE).connect("b0", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        Set<Attribute> attrs = OperatorGroup.getAttributes(s0, copyOf(detail, mock.get("o0")));
        assertThat(attrs, hasItem(Attribute.CONSUMER));
    }

    /**
     * attributes - input/output.
     */
    @Test
    public void attributes_io() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));

        Set<Attribute> a0 = OperatorGroup.getAttributes(s0, copyOf(detail, mock.get("b0")));
        Set<Attribute> a1 = OperatorGroup.getAttributes(s0, copyOf(detail, mock.get("e0")));
        assertThat(a0, hasItem(Attribute.INPUT));
        assertThat(a0, hasItem(Attribute.BEGIN));
        assertThat(a1, hasItem(Attribute.OUTPUT));
        assertThat(a1, hasItem(Attribute.END));
    }

    /**
     * attributes - markers.
     */
    @Test
    public void attributes_markers() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("c0", PlanMarker.CHECKPOINT).connect("b0", "c0")
            .marker("c1", PlanMarker.BROADCAST).connect("b0", "c1")
            .operator("o0", "i0,i1", "o0").connect("c0", "o0.i0").connect("c1", "o0.i1")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("c0", "c1"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        Set<Attribute> a0 = OperatorGroup.getAttributes(s0, copyOf(detail, mock.get("c0")));
        Set<Attribute> a1 = OperatorGroup.getAttributes(s0, copyOf(detail, mock.get("c1")));
        assertThat(a0, hasItem(Attribute.CHECKPOINT));
        assertThat(a1, hasItem(Attribute.BROADCAST));
    }

    /**
     * redundant output elimination - simple case.
<pre>{@code
b0 +-- o0 --- e0
    \- o1 --- e1
==>
b0 +-- o0 +-- e0
    \- o1  \- e1

[=]
b0 +-- o0 +-- e0
           \- e1
}</pre>
     */
    @Test
    public void redundant_output_elimination_simple() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .operator("o1").connect("b0", "o1")
            .marker("e0", PlanMarker.END).connect("o0", "e0")
            .marker("e1", PlanMarker.END).connect("o1", "e1");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("e0", "e1"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0", "o1")));
        assertThat(group.toString(), group.applyRedundantOutputElimination(), is(true));
        assertThat(group.toString(), group.applyRedundantOutputElimination(), is(false));

        Set<Operator> e0preds = Operators.getPredecessors(copyOf(detail, mock.get("e0")));
        assertThat(e0preds, hasSize(1));
        Set<Operator> e1preds = Operators.getPredecessors(copyOf(detail, mock.get("e1")));
        assertThat(e1preds, equalTo(e0preds));
    }

    /**
     * redundant output elimination - different upstreams.
<pre>{@code
b0 --- o0 --- e0
b1 --- o1 --- e1
}</pre>
     */
    @Test
    public void redundant_output_elimination_different_upstreams() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .operator("o1").connect("b1", "o1")
            .marker("e0", PlanMarker.END).connect("o0", "e0")
            .marker("e1", PlanMarker.END).connect("o1", "e1");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0", "b1"), mock.getAsSet("e0", "e1"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0", "o1")));
        assertThat(group.applyRedundantOutputElimination(), is(false));
    }

    /**
     * redundant output elimination - sub-plan io.
<pre>{@code
b0 +-- c0 ===> c0 --- e0
    \- c1 ===> c1 --- e1
==>
[1]
b0 +-- c0 +==> c0 --- e0
    \- c1  \=> c1 --- e1

[2]
b0 +-- c0 +==> c0 +-- e0
    \- c1  \=> c1  \- e1

[=]
b0 --- c0 ===> c0 +-- e0
                   \- e1
}</pre>
     */
    @Test
    public void redundant_output_elimination_io() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("c0", PlanMarker.CHECKPOINT).connect("b0", "c0")
            .marker("c1", PlanMarker.CHECKPOINT).connect("b0", "c1")
            .marker("e0", PlanMarker.END).connect("c0", "e0")
            .marker("e1", PlanMarker.END).connect("c1", "e1");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("c0", "c1"))
            .add(mock.getAsSet("c0", "c1"), mock.getAsSet("e0", "e1"))
            .build();

        // -> redundant output
        SubPlan s0 = ownerOf(detail, mock.get("b0"));
        OperatorGroup g0 = group(s0, toCopies(detail, s0, mock.getAsSet("c0", "c1")));
        assertThat(g0.applyRedundantOutputElimination(), is(true));
        assertThat(g0.applyRedundantOutputElimination(), is(false));

        // -> redundant input
        SubPlan s1 = ownerOf(detail, mock.get("e0"));
        OperatorGroup g1 = group(s1, toCopies(detail, s1, mock.getAsSet("c0", "c1")));
        assertThat(g1.applyRedundantOutputElimination(), is(true));
        assertThat(g1.applyRedundantOutputElimination(), is(false));

        Set<Operator> e0preds = Operators.getPredecessors(copyOf(detail, mock.get("e0")));
        assertThat(e0preds, hasSize(1));
        Set<Operator> e1preds = Operators.getPredecessors(copyOf(detail, mock.get("e1")));
        assertThat(e1preds, equalTo(e0preds));
    }

    /**
     * union push down - simple case.
<pre>{@code
b0 --- o0 --+ e0
b1 --- o1 -/
==>
b0 --+ o0 --+ e0
b1 -/  o1 -/
[=]
b0 --+ o0 --- e0
b1 -/
}</pre>
     */
    @Test
    public void union_push_down_simple() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .operator("o1").connect("b1", "o1")
            .marker("e0", PlanMarker.END).connect("o0", "e0").connect("o1", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0", "b1"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0", "o1")));
        assertThat(group.toString(), group.applyUnionPushDown(), is(true));
        assertThat(group.toString(), group.applyUnionPushDown(), is(false));

        Set<Operator> b0succs = Operators.getSuccessors(copyOf(detail, mock.get("b0")));
        assertThat(b0succs, hasSize(1));
        Set<Operator> b1succs = Operators.getSuccessors(copyOf(detail, mock.get("b1")));
        assertThat(b1succs, equalTo(b0succs));
    }

    /**
     * union push down - different output.
<pre>{@code
b0 --- o0 --- e0
b1 --- o1 --- e1
}</pre>
     */
    @Test
    public void union_push_down_different_output() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .operator("o1").connect("b1", "o1")
            .marker("e0", PlanMarker.END).connect("o0", "e0")
            .marker("e1", PlanMarker.END).connect("o1", "e1");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0", "b1"), mock.getAsSet("e0", "e1"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0", "o1")));
        assertThat(group.applyUnionPushDown(), is(false));
    }

    /**
     * union push down - inputs/outputs.
<pre>{@code
b0 --- c0 ===> c0 --+ e0
b1 --- c1 ===> c1 -/
>>>
[1]
b0 --- c0 ==+> c0 --+ e0
b1 --- c1 =/   c1 -/
[2]
b0 --+ c0 ==+> c0 --+ e0
b1 -/  c1 =/   c1 -/
[=]
b0 --+ c0 ==+> c0 --+ e0
b1 -/
}</pre>
     */
    @Test
    public void union_push_down_io() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .marker("c0", PlanMarker.CHECKPOINT).connect("b0", "c0")
            .marker("c1", PlanMarker.CHECKPOINT).connect("b1", "c1")
            .marker("e0", PlanMarker.END).connect("c0", "e0").connect("c1", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0", "b1"), mock.getAsSet("c0", "c1"))
            .add(mock.getAsSet("c0", "c1"), mock.getAsSet("e0"))
            .build();

        // -> [1] redundant input
        SubPlan s1 = ownerOf(detail, mock.get("e0"));
        OperatorGroup g1 = group(s1, toCopies(detail, s1, mock.getAsSet("c0", "c1")));
        assertThat(g1.applyUnionPushDown(), is(true));
        assertThat(g1.applyUnionPushDown(), is(false));

        // -> [2] redundant output
        SubPlan s0 = ownerOf(detail, mock.get("b0"));
        OperatorGroup g0 = group(s0, toCopies(detail, s0, mock.getAsSet("c0", "c1")));
        assertThat(g0.applyUnionPushDown(), is(true));
        assertThat(g0.applyUnionPushDown(), is(false));

        Set<Operator> b0succs = Operators.getSuccessors(copyOf(detail, mock.get("b0")));
        assertThat(b0succs, hasSize(1));
        Set<Operator> b1succs = Operators.getSuccessors(copyOf(detail, mock.get("b1")));
        assertThat(b1succs, equalTo(b0succs));
    }

    /**
     * union push down - w/ same broadcast.
     */
    @Test
    public void union_push_down_w_same_broadcast() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .marker("b2", PlanMarker.BEGIN)
            .marker("c0", PlanMarker.BROADCAST).connect("b2", "c0")
            .operator("o0", "i0,i1", "o0").connect("b0", "o0.i0").connect("c0", "o0.i1")
            .operator("o1", "i0,i1", "o0").connect("b1", "o1.i0").connect("c0", "o1.i1")
            .marker("e0", PlanMarker.END).connect("o0", "e0").connect("o1", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b2"), mock.getAsSet("c0"))
            .add(mock.getAsSet("b0", "b1", "c0"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0", "o1")), 1);
        assertThat(group.applyUnionPushDown(), is(true));
        assertThat(group.applyUnionPushDown(), is(false));

        Set<Operator> b0succs = Operators.getSuccessors(copyOf(detail, mock.get("b0")));
        assertThat(b0succs, hasSize(1));
        Set<Operator> b1succs = Operators.getSuccessors(copyOf(detail, mock.get("b1")));
        assertThat(b1succs, equalTo(b0succs));
    }

    /**
     * union push down - w/ different broadcast.
     */
    @Test
    public void union_push_down_w_diff_broadcast() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .marker("b2", PlanMarker.BEGIN)
            .marker("c0", PlanMarker.BROADCAST).connect("b2", "c0")
            .marker("c1", PlanMarker.BROADCAST).connect("b2", "c1")
            .operator("o0", "i0,i1", "o0").connect("b0", "o0.i0").connect("c0", "o0.i1")
            .operator("o1", "i0,i1", "o0").connect("b1", "o1.i0").connect("c1", "o1.i1")
            .marker("e0", PlanMarker.END).connect("o0", "e0").connect("o1", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b2"), mock.getAsSet("c0", "c1"))
            .add(mock.getAsSet("b0", "b1", "c0", "c1"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0", "o1")), 1);
        assertThat(group.applyUnionPushDown(), is(false));
    }

    /**
     * union push down - generators.
     */
    @Test
    public void union_push_down_generator() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .operator("o0", OperatorConstraint.GENERATOR).connect("b0", "o0")
            .operator("o1", OperatorConstraint.GENERATOR).connect("b1", "o1")
            .marker("e0", PlanMarker.END).connect("o0", "e0").connect("o1", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0", "b1"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0", "o1")));
        assertThat(group.applyUnionPushDown(), is(false));
    }

    /**
     * union push down - non-extract operators.
     */
    @Test
    public void union_push_down_non_extract() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .marker("g0", PlanMarker.GATHER).connect("b0", "g0")
            .marker("g1", PlanMarker.GATHER).connect("b1", "g1")
            .operator("o0").connect("g0", "o0")
            .operator("o1").connect("g1", "o1")
            .marker("e0", PlanMarker.END).connect("o0", "e0").connect("o1", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0", "b1"), mock.getAsSet("g0", "g1"))
            .add(mock.getAsSet("g0", "g1"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0", "o1")));
        assertThat(group.applyUnionPushDown(), is(false));
    }

    /**
     * apply trivial output elimination - simple case.
<pre>{@code
() ===> b0 --- o0 --- e0
>>>
() ===> b0 +--------- e0
            \- o0
[=]
() ===> b0 --- e0
}</pre>
     */
    @Test
    public void apply_trivial_output_elimination_simple() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0")));
        assertThat(group.toString(), group.applyTrivialOutputElimination(), is(true));
        assertThat(group.toString(), group.applyTrivialOutputElimination(), is(false));

        Set<Operator> preds = Operators.getPredecessors(copyOf(detail, mock.get("e0")));
        assertThat(toSources(detail, preds), hasOperators("b0"));
    }

    /**
     * apply trivial output elimination - non trivial case.
<pre>{@code
... c0 ===> c0 --- o0 --- e0
}</pre>
     */
    @Test
    public void apply_trivial_output_elimination_non_trivial() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("c0", PlanMarker.CHECKPOINT).connect("b0", "c0")
            .operator("o0").connect("c0", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("c0"))
            .add(mock.getAsSet("c0"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0")));
        assertThat(group.applyTrivialOutputElimination(), is(false));
    }

    /**
     * apply trivial output elimination - simple case.
<pre>{@code
() ===> b0 --+ o0 --- e0
() ===> b1 -/
() ===> b2 -/
>>>
[=]
() ===> b0 --- e0
() ===> b1
() ===> b2
}</pre>
     */
    @Test
    public void apply_trivial_output_elimination_many() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .marker("b2", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0").connect("b1", "o0").connect("b2", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0", "b1", "b2"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0")));
        assertThat(group.applyTrivialOutputElimination(), is(true));
        assertThat(group.applyTrivialOutputElimination(), is(false));

        Set<Operator> preds = Operators.getPredecessors(copyOf(detail, mock.get("e0")));
        assertThat(preds, contains(anyOf(
                is(copyOf(detail, mock.get("b0"))),
                is(copyOf(detail, mock.get("b1"))),
                is(copyOf(detail, mock.get("b2"))))));
    }

    /**
     * apply trivial output elimination - partially trivial.
<pre>{@code
   () ===> b0 -\
.. c0 ===> c0 --+ o0 --- e0
>>>
   () ===> b0
.. c0 ===> c0 --- o0 --- e0
[=]
.. c0 ===> c0 --- o0 --- e0
}</pre>
     */
    @Test
    public void apply_trivial_output_elimination_partial() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .marker("c0", PlanMarker.BEGIN).connect("b1", "c0")
            .operator("o0").connect("b0", "o0").connect("c0", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0", "c0"), mock.getAsSet("e0"))
            .add(mock.getAsSet("b1"), mock.getAsSet("c0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0")));
        assertThat(group.applyTrivialOutputElimination(), is(true));
        assertThat(group.applyTrivialOutputElimination(), is(false));

        Set<Operator> o0preds = Operators.getPredecessors(copyOf(detail, mock.get("o0")));
        assertThat(toSources(detail, o0preds), hasOperators("c0"));

        Set<Operator> e0preds = Operators.getPredecessors(copyOf(detail, mock.get("e0")));
        assertThat(toSources(detail, e0preds), hasOperators("o0"));
    }

    /**
     * apply trivial output elimination - w/ broadcast.
<pre>{@code
   () ===> b0 --+ o0 --- e0
.. c0 ===> c0 -/
:: where c0 is BROADCAST
>>>
[=]
() ===> b0 --- e0
}</pre>
     */
    @Test
    public void apply_trivial_output_elimination_broadcast() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .marker("c0", PlanMarker.BROADCAST).connect("b1", "c0")
            .operator("o0", "i0,i1", "o0").connect("b0", "o0.i0").connect("c0", "o0.i1")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0", "c0"), mock.getAsSet("e0"))
            .add(mock.getAsSet("b1"), mock.getAsSet("c0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0")), 1);
        assertThat(group.applyTrivialOutputElimination(), is(true));
        assertThat(group.applyTrivialOutputElimination(), is(false));

        Set<Operator> preds = Operators.getPredecessors(copyOf(detail, mock.get("e0")));
        assertThat(toSources(detail, preds), hasOperators("b0"));
    }

    /**
     * apply trivial output elimination - generator.
<pre>{@code
() ===> b0 --+ o0 --- e0
() ===> b1 -/
>>>
[=]
() ===> b0 --- o0 --- e0
() ===> b1
}</pre>
     */
    @Test
    public void apply_trivial_output_elimination_generator() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("b1", PlanMarker.BEGIN)
            .marker("b2", PlanMarker.BEGIN)
            .operator("o0", OperatorConstraint.GENERATOR).connect("b0", "o0").connect("b1", "o0").connect("b2", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0", "b1", "b2"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("o0"));
        OperatorGroup group = group(s0, toCopies(detail, mock.getAsSet("o0")));
        assertThat(group.applyTrivialOutputElimination(), is(true));
        assertThat(group.applyTrivialOutputElimination(), is(false));

        Set<Operator> o0preds = Operators.getPredecessors(copyOf(detail, mock.get("o0")));
        assertThat(o0preds, contains(anyOf(
                is(copyOf(detail, mock.get("b0"))),
                is(copyOf(detail, mock.get("b1"))),
                is(copyOf(detail, mock.get("b2"))))));

        Set<Operator> e0preds = Operators.getPredecessors(copyOf(detail, mock.get("e0")));
        assertThat(toSources(detail, e0preds), hasOperators("o0"));
    }

    /**
     * apply trivial output elimination - inputs/outputs.
     */
    @Test
    public void apply_trivial_output_elimination_io() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .operator("o0").connect("b0", "o0")
            .marker("e0", PlanMarker.END).connect("o0", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("e0"))
            .build();

        SubPlan s0 = ownerOf(detail, mock.get("b0"));
        OperatorGroup g0 = group(s0, toCopies(detail, mock.getAsSet("b0")));
        assertThat(g0.applyTrivialOutputElimination(), is(false));

        SubPlan s1 = ownerOf(detail, mock.get("e0"));
        OperatorGroup g1 = group(s1, toCopies(detail, mock.getAsSet("e0")));
        assertThat(g1.applyTrivialOutputElimination(), is(false));
    }

    /**
     * apply duplicate checkpoint elimination - simple case.
<pre>{@code
... c0 ===> c0 +--------+ c1 ===> c1 ...
                \- o0 -/
>>>
... c0 +=======================+> c1 ...
        \=> c0 --- o0 --- c1 =/

[=]
... c0 ===> c1 ...
}</pre>
     */
    @Test
    public void apply_duplicate_checkpoint_elimination_simple() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("c0", PlanMarker.CHECKPOINT).connect("b0", "c0")
            .operator("o0").connect("c0", "o0")
            .marker("c1", PlanMarker.CHECKPOINT).connect("c0", "c1").connect("o0", "c1")
            .marker("e0", PlanMarker.END).connect("c1", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("c0"))
            .add(mock.getAsSet("c0"), mock.getAsSet("c1"))
            .add(mock.getAsSet("c1"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("b0"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));
        SubPlan s2 = ownerOf(detail, mock.get("e0"));
        OperatorGroup group = group(s1, toCopies(detail, s1, mock.getAsSet("c0")));
        assertThat(group.toString(), group.applyDuplicateCheckpointElimination(), is(true));
        assertThat(group.toString(), group.applyDuplicateCheckpointElimination(), is(false));

        assertThat(pred(s0), is(empty()));
        assertThat(pred(s1), containsInAnyOrder(s0));
        assertThat(pred(s2), containsInAnyOrder(s0, s1));
    }

    /**
     * apply duplicate checkpoint elimination - not a target.
     */
    @Test
    public void apply_duplicate_checkpoint_elimination_others() {
        MockOperators mock = new MockOperators()
            .marker("b0", PlanMarker.BEGIN)
            .marker("c0", PlanMarker.CHECKPOINT).connect("b0", "c0")
            .operator("o0").connect("c0", "o0")
            .marker("c1", PlanMarker.CHECKPOINT).connect("c0", "c1").connect("o0", "c1")
            .marker("e0", PlanMarker.END).connect("c1", "e0");
        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("b0"), mock.getAsSet("c0"))
            .add(mock.getAsSet("c0"), mock.getAsSet("c1"))
            .add(mock.getAsSet("c1"), mock.getAsSet("e0"))
            .build();
        SubPlan s0 = ownerOf(detail, mock.get("b0"));
        SubPlan s1 = ownerOf(detail, mock.get("o0"));
        SubPlan s2 = ownerOf(detail, mock.get("e0"));

        // INPUT but not CHECKPOINT
        OperatorGroup g0 = group(s0, toCopies(detail, s0, mock.getAsSet("b0")));
        assertThat(g0.applyDuplicateCheckpointElimination(), is(false));

        // CHECKPOINT but not INPUT
        OperatorGroup g1 = group(s0, toCopies(detail, s0, mock.getAsSet("c0")));
        assertThat(g1.applyDuplicateCheckpointElimination(), is(false));

        // normal operators
        OperatorGroup g2 = group(s1, toCopies(detail, s1, mock.getAsSet("o0")));
        assertThat(g2.applyDuplicateCheckpointElimination(), is(false));

        // CHECKPOINT -> not CHECKPOINT
        OperatorGroup g3 = group(s2, toCopies(detail, s2, mock.getAsSet("c1")));
        assertThat(g3.applyDuplicateCheckpointElimination(), is(false));
    }

    private OperatorGroup group(SubPlan sub, Set<Operator> operators) {
        return group(sub, operators, new int[0]);
    }

    private OperatorGroup group(SubPlan sub, Set<Operator> operators, int... broadcastInputs) {
        BitSet bits = new BitSet();
        for (int i : broadcastInputs) {
            bits.set(i);
        }
        Set<Attribute> attributes = OperatorGroup.getAttributes(sub, operators.iterator().next());
        return new OperatorGroup((BasicSubPlan) sub, operators, bits, attributes);
    }
}
