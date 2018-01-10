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
    private final Map<Operator, Occurrence> sourceMap;

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
                            "operator \"{0}\" must be unique: {1} <=> {2}", //$NON-NLS-1$
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
        Occurrence occurrence = sourceMap.get(source);
        if (occurrence == null) {
            occurrence = new Occurrence();
            sourceMap.put(source, occurrence);
        }
        occurrence.add(sub, copy);
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
        Occurrence occurrence = sourceMap.get(source);
        if (occurrence == null) {
            return Collections.emptySet();
        }
        return occurrence.getCopies();
    }

    /**
     * Returns the copy of operators into this plan from the source operator.
     * @param source the original operator
     * @param owner the target sub-plan which the operator copies into
     * @return the copy of operator, or {@code null} if there is no such an operator
     * @see #getSource(Operator)
     */
    public Set<Operator> getCopies(Operator source, SubPlan owner) {
        Occurrence occurrence = sourceMap.get(source);
        if (occurrence == null) {
            return Collections.emptySet();
        }
        return occurrence.getCopies(owner);
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
                        "the specified operator must be a copy (it seems source operator): {0}", //$NON-NLS-1$
                        copy));
            }
            return null;
        }
        return mapping.owner;
    }

    private static final class Mapping {

        final Operator source;

        final SubPlan owner;

        Mapping(Operator source, SubPlan owner) {
            this.source = source;
            this.owner = owner;
        }
    }

    private static final class Occurrence {

        private final Map<SubPlan, Set<Operator>> ownerAndCopies = new HashMap<>(1);

        Occurrence() {
            return;
        }

        void add(SubPlan owner, Operator copy) {
            Set<Operator> copies = ownerAndCopies.get(owner);
            if (copies == null) {
                copies = new HashSet<>(1);
                ownerAndCopies.put(owner, copies);
            }
            copies.add(copy);
        }

        Set<Operator> getCopies() {
            if (ownerAndCopies.isEmpty()) {
                return Collections.emptySet();
            }
            Set<Operator> results = new HashSet<>();
            for (Set<Operator> copies : ownerAndCopies.values()) {
                results.addAll(copies);
            }
            return Collections.unmodifiableSet(results);
        }

        Set<Operator> getCopies(SubPlan owner) {
            Set<Operator> copies = ownerAndCopies.get(owner);
            if (copies == null) {
                return Collections.emptySet();
            }
            return Collections.unmodifiableSet(copies);
        }
    }
}
