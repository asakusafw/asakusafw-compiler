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

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

/**
 * Optimizes {@link BasicPlan}s.
 */
public class BasicPlanOptimizer {

    private final Set<Option> options = EnumSet.noneOf(Option.class);

    /**
     * Creates a new instance.
     * @param options the optimizer options
     */
    public BasicPlanOptimizer(Collection<? extends Option> options) {
        this.options.addAll(options);
    }

    /**
     * Optimizes the target plan.
     * @param plan the target plan
     */
    public void optimize(BasicPlan plan) {
        if (options.isEmpty()) {
            return;
        }
        BasicPlanEditor editor = new BasicPlanEditor(plan);
        boolean changed;
        do {
            changed = doOptimize(editor);
        } while (changed);
    }

    private boolean doOptimize(BasicPlanEditor editor) {
        boolean changed = false;
        if (options.contains(Option.REDUNDANT_OUTPUT_ELIMINATION)) {
            changed |= applyRedundantOutputElimination(editor);
        }
        if (options.contains(Option.UNION_PUSH_DOWN)) {
            changed |= applyUnionPushDown(editor);
        }
        if (options.contains(Option.TRIVIAL_OUTPUT_ELIMINATION)) {
            changed |= applyTrivialOutputElimination(editor);
        }
        if (options.contains(Option.DUPLICATE_CHECKPOINT_ELIMINATION)) {
            changed |= applyDuplicateCheckpointElimination(editor);
        }
        changed |= editor.revalidate();
        return changed;
    }

    private boolean applyRedundantOutputElimination(BasicPlanEditor editor) {
        boolean changed = false;
        for (BasicSubPlanEditor sub : editor.getSubEditorsForward()) {
            for (OperatorGroup group : sub.getOperatorGroupsForward()) {
                changed |= group.applyRedundantOutputElimination();
            }
        }
        return changed;
    }

    private boolean applyUnionPushDown(BasicPlanEditor editor) {
        boolean changed = false;
        for (BasicSubPlanEditor sub : editor.getSubEditorsBackward()) {
            for (OperatorGroup group : sub.getOperatorGroupsBackward()) {
                changed |= group.applyUnionPushDown();
            }
        }
        return changed;
    }

    private boolean applyTrivialOutputElimination(BasicPlanEditor editor) {
        boolean changed = false;
        for (BasicSubPlanEditor sub : editor.getSubEditorsForward()) {
            for (OperatorGroup group : sub.getOperatorGroupsForward()) {
                changed |= group.applyTrivialOutputElimination();
            }
        }
        return changed;
    }

    private boolean applyDuplicateCheckpointElimination(BasicPlanEditor editor) {
        boolean changed = false;
        for (BasicSubPlanEditor sub : editor.getSubEditorsForward()) {
            for (OperatorGroup group : sub.getOperatorGroupsForward()) {
                changed |= group.applyDuplicateCheckpointElimination();
            }
        }
        return changed;
    }

    /**
     * Options for {@link BasicPlanOptimizer}.
     */
    public enum Option {

        /**
         * Eliminates operator which output is always an empty data-set.
         * This never removes sub-plan input and output ports.
         * @see #DUPLICATE_CHECKPOINT_ELIMINATION
         */
        TRIVIAL_OUTPUT_ELIMINATION,

        /**
         * Eliminates operators which output is equivalent to an other output,
         * and successors will use the latter instead of the former.
         */
        REDUNDANT_OUTPUT_ELIMINATION,

        /**
         * Eliminates checkpoint operation which just follows another checkpoint operation.
         */
        DUPLICATE_CHECKPOINT_ELIMINATION,

        /**
         * Push-down {@code union} operations.
         */
        UNION_PUSH_DOWN,
    }
}
