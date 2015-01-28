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
package com.asakusafw.lang.compiler.planning;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.Operator;

/**
 * Detail information for {@link Plan} for re-organizing execution plans.
 */
public class PlanDetail {

    private final Plan plan;

    // source -> copy*
    private final Map<Operator, Map<SubPlan, Operator>> sourceMap;

    // copy -> (source, sub-plan)
    private final Map<Operator, Mapping> copyMap;

    /**
     * Creates a new instance.
     * @param plan the target plan
     * @param mapping mapping information from the copy of operators appeared in sub-plan to the source operators
     */
    public PlanDetail(Plan plan, Map<? extends Operator, ? extends Operator> mapping) {
        this.plan = plan;
        this.sourceMap = new HashMap<>();
        this.copyMap = new HashMap<>();
        for (SubPlan sub : plan.getElements()) {
            for (Operator copy : sub.getOperators()) {
                Operator source = mapping.get(copy);
                if (copyMap.containsKey(copy)) {
                    Mapping conflict = copyMap.get(copy);
                    throw new IllegalArgumentException(MessageFormat.format(
                            "operator \"{0}\" must be unique: {1} <=> {2}",
                            copy,
                            conflict.owner,
                            sub));
                }
                copyMap.put(copy, new Mapping(source, sub));
                if (source != null) {
                    addSourceMap(source, sub, copy);
                }
            }
        }
    }

    private void addSourceMap(Operator source, SubPlan sub, Operator copy) {
        Map<SubPlan, Operator> copies = sourceMap.get(source);
        if (copies == null) {
            sourceMap.put(source, Collections.singletonMap(sub, copy));
        } else if (copies.size() == 1) {
            Map<SubPlan, Operator> newCopies = new HashMap<>();
            newCopies.putAll(copies);
            assert newCopies.containsKey(sub) == false : sub;
            newCopies.put(sub, copy);
            sourceMap.put(source, newCopies);
        } else {
            assert copies.containsKey(sub) == false : sub;
            copies.put(sub, copy);
        }
    }

    /**
     * Returns the target plan.
     * @return the plan
     */
    public Plan getPlan() {
        return plan;
    }

    /**
     * Returns the all source operators.
     * @return the all source operators
     */
    public Set<Operator> getSources() {
        return Collections.unmodifiableSet(sourceMap.keySet());
    }

    /**
     * Returns the all copies from source operators.
     * @return the all copies from source operators
     */
    public Set<Operator> getCopies() {
        return Collections.unmodifiableSet(copyMap.keySet());
    }

    /**
     * Returns the copy of operators into this plan from the source operator.
     * @param source the original operator
     * @return the copy of operators, or an empty set if there are no such operators
     * @see #getSource(Operator)
     */
    public Set<Operator> getCopies(Operator source) {
        Map<SubPlan, Operator> copies = sourceMap.get(source);
        if (copies == null) {
            return Collections.emptySet();
        }
        return new HashSet<>(copies.values());
    }

    /**
     * Returns the copy of operators into this plan from the source operator.
     * @param source the original operator
     * @param owner the target sub-plan which the operator copies into
     * @return the copy of operator, or {@code null} if there is no such an operator
     * @see #getSource(Operator)
     */
    public Operator getCopy(Operator source, SubPlan owner) {
        Map<SubPlan, Operator> copies = sourceMap.get(source);
        if (copies == null) {
            return null;
        }
        return copies.get(owner);
    }

    /**
     * Returns the source operator from its copy of operator.
     * @param copy a copy of the source operator
     * @return the corresponding source operator, or {@code null} if the source is not found
     * @see #getCopies(Operator)
     */
    public Operator getSource(Operator copy) {
        Mapping mapping = copyMap.get(copy);
        if (mapping == null) {
            return null;
        }
        return mapping.source;
    }

    /**
     * Returns a sub-plan which contains the target operator.
     * The operator must be a copy, and must not be a source operator.
     * @param copy a copy of operator which appears in the sub-plan
     * @return the owner sub-plan, or {@code null} if the sub-plan is not found
     * @see #getCopies(Operator)
     */
    public SubPlan getOwner(Operator copy) {
        Mapping mapping = copyMap.get(copy);
        if (mapping == null) {
            if (sourceMap.containsKey(copy)) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "the specified operator must be a copy (it seems source operator): {0}",
                        copy));
            }
            return null;
        }
        return mapping.owner;
    }

    private static final class Mapping {

        final Operator source;

        final SubPlan owner;

        Mapping(Operator source, SubPlan appearance) {
            this.source = source;
            this.owner = appearance;
        }
    }
}
