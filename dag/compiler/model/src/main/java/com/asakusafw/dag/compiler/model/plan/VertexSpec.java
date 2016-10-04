/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.model.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * Extra information for {@link SubPlan}.
 * @since 0.4.0
 */
public class VertexSpec implements ElementSpec<SubPlan> {

    private final SubPlan origin;

    private final String id;

    private final OperationType operationType;

    private final Set<OperationOption> operationOptions;

    private final Operator primaryOperator;

    /**
     * Returns the spec of the target element.
     * @param origin the target element
     * @return the spec
     */
    public static VertexSpec get(SubPlan origin) {
        Arguments.requireNonNull(origin);
        return Invariants.requireNonNull(origin.getAttribute(VertexSpec.class));
    }

    /**
     * Creates a new instance.
     * @param origin the original sub-plan
     * @param id the vertex ID
     * @param primaryOperator the primary operator (nullable)
     * @param operationType the operation type
     * @param operationOptions the operation options
     */
    public VertexSpec(
            SubPlan origin,
            String id,
            OperationType operationType,
            Collection<OperationOption> operationOptions,
            Operator primaryOperator) {
        Arguments.requireNonNull(origin);
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(operationType);
        Arguments.requireNonNull(operationOptions);
        this.origin = origin;
        this.id = id;
        this.operationType = operationType;
        this.operationOptions = EnumUtil.freeze(operationOptions);
        this.primaryOperator = primaryOperator;
    }

    @Override
    public SubPlan getOrigin() {
        return origin;
    }

    @Override
    public String getId() {
        return id;
    }

    /**
     * Returns the operation type.
     * @return the operation type
     */
    public OperationType getOperationType() {
        return operationType;
    }

    /**
     * Returns the operation options.
     * @return the operation options
     */
    public Set<OperationOption> getOperationOptions() {
        return operationOptions;
    }

    /**
     * Returns the primary operator of the target sub-plan.
     * @return the primary operator, or {@code null} if the driver is {@link OperationType#EXTRACT}
     */
    public Operator getPrimaryOperator() {
        return primaryOperator;
    }

    /**
     * Returns the label of this information.
     * @return the label (never null)
     */
    public String getLabel() {
        Operator typical = getTypicalOperator();
        if (typical == null) {
            return "Id"; //$NON-NLS-1$
        } else {
            return Util.toOperatorLabel(typical);
        }
    }

    private Operator getTypicalOperator() {
        if (primaryOperator != null) {
            return primaryOperator;
        }
        List<Operator> candidates = new ArrayList<>();
        for (SubPlan.Input input : origin.getInputs()) {
            if (Util.isPrimary(input) == false) {
                continue;
            }
            for (Operator operator : Operators.getSuccessors(input.getOperator())) {
                if (origin.findOutput(operator) != null) {
                    continue;
                }
                candidates.add(operator);
            }
        }
        return Util.findMostTypical(candidates);
    }

    @Override
    public Map<String, ?> toMap() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("id", getId()); //$NON-NLS-1$
        results.put("type", getOperationType()); //$NON-NLS-1$
        results.put("primary", Util.toOperatorLabel(getPrimaryOperator())); //$NON-NLS-1$
        results.put("options", getOperationOptions()); //$NON-NLS-1$
        return results;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }

    /**
     * Represents an operation type.
     * @since 0.4.0
     */
    public enum OperationType {

        /**
         * Extract operation.
         */
        EXTRACT,

        /**
         * Co-group operation.
         */
        CO_GROUP,

        /**
         * Output operation.
         */
        OUTPUT,
    }

    /**
     * Represents an operation options.
     * @since 0.4.0
     */
    public enum OperationOption {

        /**
         * Obtains data from the external input.
         */
        EXTERNAL_INPUT,

        /**
         * Prepares data for the external output.
         */
        EXTERNAL_OUTPUT,

        /**
         * Whether the primary operator requires the pre-aggregated input.
         */
        PRE_AGGREGATION,
    }
}
