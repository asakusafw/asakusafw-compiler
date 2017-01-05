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

import java.util.Set;

import com.asakusafw.lang.compiler.common.AttributeContainer;

/**
 * Represents an execution plan.
 * <p>
 * Each {@link Plan} object must satisfy following invariants:
 * </p>
 * <ol>
 * <li> each sub-plan satisfies its {@link SubPlan invariants} </li>
 * <li> dependency graph of the sub-plans is acyclic </li>
 * </ol>
 * @see PlanBuilder
 */
public interface Plan extends AttributeContainer {

    /**
     * Returns sub-plans in this execution plan.
     * @return sub-plans
     */
    Set<? extends SubPlan> getElements();
}
