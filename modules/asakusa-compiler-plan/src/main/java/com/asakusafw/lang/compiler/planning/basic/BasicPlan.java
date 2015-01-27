/**
 * Copyright 2011-2015 Asakusa Framework Team.
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

import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * A basic implementation of {@link Plan}.
 */
public class BasicPlan extends AbstractAttributeContainer implements Plan {

    private final Set<BasicSubPlan> elements = new LinkedHashSet<>();

    @Override
    public Set<BasicSubPlan> getElements() {
        return new LinkedHashSet<>(elements);
    }

    /**
     * Adds a new sub-plan to this.
     * The specified operators must satisfy following preconditions:
     * <ol>
     * <li> each input operator must be a plan marker </li>
     * <li> each input operator must not have any predecessors </li>
     * <li> each input operator must be forward reachable to at least one output operator </li>
     * <li> each output operator must be a plan marker </li>
     * <li> each output operator must not have any successors </li>
     * <li> each output operator must be backward reachable to at least one input operator </li>
     * </ol>
     *
     * Here, we introduce the term {@code body operators} which are transitive connected to
     * any sub-plan inputs or outputs (excludes inputs or outputs themselves).
     * And then the body operators must satisfy following preconditions:
     * <ol>
     * <li> each body operator must not be a plan marker </li>
     * <li> each body operator must be backward reachable to at least one input operator </li>
     * <li> each body operator must be forward reachable to at least one output operator </li>
     * </ol>
     *
     * The created sub-plan will have following properties:
     * <ol>
     * <li> each input operator becomes as its {@link SubPlan#getInputs() sub-plan input} </li>
     * <li> each output operator becomes as its {@link SubPlan#getOutputs() sub-plan output} </li>
     * <li> {@link SubPlan#getOwner()} returns this {@link BasicPlan} </li>
     * <li> {@link SubPlan#getOperators()} returns union of the input, output, and body operators </li>
     * <li> each sub-plan input does not have any opposites </li>
     * <li> each sub-plan outputs does not have any opposites </li>
     * </ol>
     *
     * Clients should not modify any operators in the sub-plan.
     * @param inputs the sub-plan input operators
     * @param outputs the sub-plan output operators
     * @return the created sub-plan
     * @see PlanMarker
     * @see PlanMarkers
     */
    public BasicSubPlan addElement(Set<? extends MarkerOperator> inputs, Set<? extends MarkerOperator> outputs) {
        BasicSubPlan element = new BasicSubPlan(this, inputs, outputs);
        elements.add(element);
        return element;
    }

    @Override
    public String toString() {
        return String.format(
                "Plan(%08x)", //$NON-NLS-1$
                hashCode());
    }
}
