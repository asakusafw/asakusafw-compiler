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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.planning.OperatorEquivalence;
import com.asakusafw.lang.compiler.planning.Planning;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * An editor for {@link BasicPlan}.
 */
final class BasicPlanEditor {

    private final BasicPlan target;

    private final List<BasicSubPlanEditor> forward = new ArrayList<>();

    private final List<BasicSubPlanEditor> backward = new ArrayList<>();

    /**
     * Creates a new instance.
     * @param target the target plan
     * @param equivalence tester for operator isomorphism
     */
    BasicPlanEditor(BasicPlan target, OperatorEquivalence equivalence) {
        this.target = target;
        Map<SubPlan, BasicSubPlanEditor> editors = new HashMap<>();
        for (BasicSubPlan s : target.getElements()) {
            editors.put(s, new BasicSubPlanEditor(s, equivalence));
        }
        Graph<SubPlan> g = Planning.toDependencyGraph(target);
        for (SubPlan s : Graphs.sortPostOrder(g)) {
            assert editors.containsKey(s);
            forward.add(editors.get(s));
        }
        for (SubPlan s : Graphs.sortPostOrder(Graphs.transpose(g))) {
            assert editors.containsKey(s);
            backward.add(editors.get(s));
        }
    }

    /**
     * Returns the editing target.
     * @return the target
     */
    public BasicPlan getTarget() {
        return target;
    }

    /**
     * Returns sub-plan editors sorted from upstream to downstream.
     * @return sub-plan editors
     */
    public List<BasicSubPlanEditor> getSubEditorsForward() {
        return forward;
    }

    /**
     * Returns sub-plan editors sorted from downstream to upstream.
     * @return sub-plan editors
     */
    public List<BasicSubPlanEditor> getSubEditorsBackward() {
        return backward;
    }

    /**
     * Re-validates the plan constraints and removes redundant elements.
     * @return {@code true} if any element was changed, otherwise {@code false}
     */
    public boolean revalidate() {
        boolean changed = false;
        for (BasicSubPlanEditor sub : forward) {
            changed |= sub.revalidate();
        }
        for (Iterator<BasicSubPlanEditor> iter = forward.iterator(); iter.hasNext();) {
            BasicSubPlanEditor sub = iter.next();
            if (sub.isEmpty()) {
                iter.remove();
                backward.remove(sub);
                target.removeElement(sub.getTarget());
                changed = true;
            }
        }
        return changed;
    }
}
