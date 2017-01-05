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
package com.asakusafw.dag.compiler.model.plan;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Extra information for {@link com.asakusafw.lang.compiler.planning.SubPlan.Input SubPlan.Input}.
 * @since 0.4.0
 */
public class InputSpec implements ElementSpec<SubPlan.Input> {

    private final SubPlan.Input origin;

    private final String id;

    private final TypeDescription dataType;

    private final InputType inputType;

    private final Set<InputOption> inputOptions;

    private final Group partitionInfo;

    /**
     * Returns the spec of the target element.
     * @param origin the target element
     * @return the spec
     */
    public static InputSpec get(SubPlan.Input origin) {
        Arguments.requireNonNull(origin);
        return Invariants.requireNonNull(origin.getAttribute(InputSpec.class));
    }

    /**
     * Creates a new instance.
     * @param origin the original sub-plan input
     * @param id the input ID
     * @param dataType the input data type
     * @param inputType the input operation type
     * @param inputOptions the extra input options
     * @param partitionInfo the input partitioning information (nullable)
     */
    public InputSpec(
            SubPlan.Input origin,
            String id,
            TypeDescription dataType,
            InputType inputType,
            Collection<InputOption> inputOptions,
            Group partitionInfo) {
        Arguments.requireNonNull(origin);
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(dataType);
        Arguments.requireNonNull(inputType);
        Arguments.requireNonNull(inputOptions);
        this.origin = origin;
        this.id = id;
        this.dataType = dataType;
        this.inputType = inputType;
        this.inputOptions = EnumUtil.freeze(inputOptions);
        this.partitionInfo = partitionInfo;
    }

    @Override
    public SubPlan.Input getOrigin() {
        return origin;
    }

    @Override
    public String getId() {
        return id;
    }

    /**
     * Returns the input data type.
     * @return the data type
     */
    public TypeDescription getDataType() {
        return dataType;
    }

    /**
     * Returns the input operation type.
     * @return the operation type
     */
    public InputType getInputType() {
        return inputType;
    }

    /**
     * Returns the extra input options.
     * @return the input options
     */
    public Set<InputOption> getInputOptions() {
        return inputOptions;
    }

    /**
     * Returns the partitioning information.
     * @return the partitioning information, or {@code null} if it is not defined
     */
    public Group getPartitionInfo() {
        return partitionInfo;
    }

    @Override
    public Map<String, ?> toMap() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("id", getId()); //$NON-NLS-1$
        results.put("type", getInputType()); //$NON-NLS-1$
        results.put("data", getDataType()); //$NON-NLS-1$
        results.put("options", getInputOptions()); //$NON-NLS-1$
        Optionals.put(results, "partition", getPartitionInfo()); //$NON-NLS-1$
        return results;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }

    /**
     * Represents an input operation type.
     * @since 0.4.0
     */
    public enum InputType {

        /**
         * No data.
         */
        NO_DATA,

        /**
         * Extract input.
         */
        EXTRACT,

        /**
         * Co-group input.
         */
        CO_GROUP,

        /**
         * Broadcast input.
         */
        BROADCAST,
    }

    /**
     * Represents an operation type.
     * @since 0.4.0
     * @version 0.4.1
     */
    public enum InputOption {

        /**
         * Represents whether the target input is a primary one or not.
         */
        PRIMARY,

        /**
         * Uses file list buffer.
         */
        SPILL_OUT,

        /**
         * Only can read once.
         * @since 0.4.1
         */
        READ_ONCE,
    }
}
