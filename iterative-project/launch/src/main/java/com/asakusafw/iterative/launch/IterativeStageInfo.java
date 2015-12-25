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
package com.asakusafw.iterative.launch;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.iterative.common.IterativeExtensions;
import com.asakusafw.iterative.common.ParameterSet;
import com.asakusafw.iterative.common.ParameterTable;

/**
 * Provides {@link StageInfo} for each round.
 * @since 0.3.0
 */
public class IterativeStageInfo {

    static final String DEFAULT_STAGE_ID_PREFIX = "round"; //$NON-NLS-1$

    private static final ParameterTable NON_ITERATIVE = IterativeExtensions.builder()
            .next() // empty parameter set
            .build();

    private final StageInfo origin;

    private final ParameterTable parameterTable;

    /**
     * Creates a new instance.
     * @param origin the original (non-iterative) stage information
     * @param parameterTable the iterative parameter table
     */
    public IterativeStageInfo(StageInfo origin, ParameterTable parameterTable) {
        Objects.requireNonNull(origin);
        Objects.requireNonNull(parameterTable);
        this.origin = origin;
        this.parameterTable = parameterTable;
    }

    /**
     * Returns the original stage information.
     * @return the original stage information
     */
    public StageInfo getOrigin() {
        return origin;
    }

    /**
     * Returns the parameter table.
     * @return the parameter table
     */
    public ParameterTable getParameterTable() {
        return parameterTable;
    }

    /**
     * Returns whether or not this stage is iterative.
     * @return {@code true} if this stage is iterative, otherwise {@code false}
     */
    public boolean isIterative() {
        return getRoundCount() > 1;
    }

    /**
     * Returns the number of planning rounds.
     * If this stage is not iterative, this method returns just {@code 1}.
     * @return the number of planning rounds
     */
    public int getRoundCount() {
        return parameterTable.isEmpty() ? 1 : parameterTable.getRowCount();
    }

    /**
     * Returns the set of available parameters all through individual rounds.
     * @return the available parameter names
     */
    public Set<String> getAvailableParameters() {
        Set<String> results = new LinkedHashSet<>();
        results.addAll(origin.getBatchArguments().keySet());
        results.addAll(parameterTable.getAvailable());
        return Collections.unmodifiableSet(results);
    }

    /**
     * Returns the set of constant parameters all through individual rounds.
     * @return the constant parameter names
     */
    public Set<String> getConstantParameters() {
        if (isIterative() == false) {
            return getAvailableParameters();
        }
        // TODO more?
        // NOTE: iterative parameters may overwrite original batch arguments
        Set<String> results = new LinkedHashSet<>();
        results.addAll(origin.getBatchArguments().keySet());
        results.removeAll(parameterTable.getAvailable());
        return results;
    }

    /**
     * Returns the set of partial parameters all through individual rounds.
     * Partial parameters will not available in some rounds.
     * @return the partial parameter names
     */
    public Set<String> getPartialParameters() {
        if (isIterative() == false) {
            return Collections.emptySet();
        }
        // NOTE: original batch arguments are behaves as default arguments
        Set<String> results = new LinkedHashSet<>();
        results.addAll(parameterTable.getPartial());
        results.removeAll(origin.getBatchArguments().keySet());
        return results;
    }

    /**
     * Returns a new cursor for iterate each round.
     * If this stage is not {@link #isIterative() iterative}, the cursor
     * @return the created cursor
     */
    public Cursor newCursor() {
        if (parameterTable.isEmpty()) {
            return new Cursor(origin, NON_ITERATIVE.newCursor());
        } else {
            return new Cursor(origin, parameterTable.newCursor());
        }
    }

    static StageInfo merge(StageInfo origin, int round, ParameterSet parameters) {
        String originalStageId = origin.getStageId() == null ? DEFAULT_STAGE_ID_PREFIX : origin.getStageId();
        String newStageId = String.format("%s_%d", originalStageId, round); //$NON-NLS-1$
        Map<String, String> newBatchArguments = new LinkedHashMap<>();
        newBatchArguments.putAll(origin.getBatchArguments());
        newBatchArguments.putAll(parameters.toMap());
        return new StageInfo(
                origin.getUserName(),
                origin.getBatchId(),
                origin.getFlowId(),
                newStageId,
                origin.getExecutionId(),
                newBatchArguments);
    }

    /**
     * A cursor over {@link IterativeStageInfo}.
     * @since 0.3.0
     */
    public static class Cursor {

        private static final int INITIAL_INDEX = 0;

        private static final int INVALID_INDEX = INITIAL_INDEX - 1;

        private final StageInfo origin;

        private final ParameterTable.Cursor parameters;

        private int index = INVALID_INDEX;

        Cursor(StageInfo origin, ParameterTable.Cursor parameters) {
            this.origin = origin;
            this.parameters = parameters;
        }

        /**
         * Advances this cursor to point to the next round, and returns whether or not the next round exists.
         * @return {@code true} if the next stage exists, otherwise {@code false}
         */
        public boolean next() {
            if (parameters.next()) {
                index++;
                return true;
            } else {
                index = INVALID_INDEX;
                return false;
            }
        }

        /**
         * Returns the stage information for the current round.
         * @return the stage information
         * @throws IllegalStateException if the cursor does not point to any elements
         */
        public StageInfo get() {
            checkRound();
            ParameterSet current = parameters.get();
            return merge(origin, index, current);
        }

        /**
         * Returns the current round index.
         * @return the current round index (0-origin)
         * @throws IllegalStateException if the cursor does not point to any elements
         */
        public int getRoundIndex() {
            checkRound();
            return index;
        }

        /**
         * Returns the changed parameter names from the previous element. If this point to the first element, the
         * returned set is equivalent to the set of available parameters in the first element.
         * If the first round, returned set will also contain constant parameter names
         * @return the changed parameter names
         * @throws IllegalStateException if the cursor does not point to any elements
         */
        public Set<String> getDifferences() {
            checkRound();
            if (index == INITIAL_INDEX) {
                Set<String> results = new LinkedHashSet<>();
                results.addAll(origin.getBatchArguments().keySet());
                results.addAll(parameters.getDifferences());
                return results;
            } else {
                return parameters.getDifferences();
            }
        }

        private void checkRound() {
            if (index == INVALID_INDEX) {
                throw new IllegalStateException();
            }
        }
    }
}
