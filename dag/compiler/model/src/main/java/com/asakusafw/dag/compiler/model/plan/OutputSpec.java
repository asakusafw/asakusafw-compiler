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
package com.asakusafw.dag.compiler.model.plan;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Extra information for {@link com.asakusafw.lang.compiler.planning.SubPlan.Output SubPlan.Output}.
 * @since 0.4.0
 */
public class OutputSpec implements ElementSpec<SubPlan.Output> {

    private final SubPlan.Output origin;

    private final String id;

    private final TypeDescription sourceType;

    private final TypeDescription dataType;

    private final OutputType outputType;

    private final Set<OutputOption> outputOptions;

    private final Group partitionInfo;

    private final Operator aggregationInfo;

    /**
     * Returns the spec of the target element.
     * @param origin the target element
     * @return the spec
     */
    public static OutputSpec get(SubPlan.Output origin) {
        Arguments.requireNonNull(origin);
        return Invariants.requireNonNull(origin.getAttribute(OutputSpec.class));
    }

    /**
     * Creates a new instance.
     * @param origin the original sub-plan output
     * @param id the output ID
     * @param sourceType the source type
     * @param wireType the output data type
     * @param outputType the output operation type
     * @param outputOptions the extra output options
     * @param partitionInfo the output partitioning information (nullable)
     * @param aggregationInfo the output pre-aggregation operator (nullable)
     */
    public OutputSpec(
            SubPlan.Output origin,
            String id,
            OutputType outputType,
            TypeDescription sourceType,
            TypeDescription wireType,
            Collection<OutputOption> outputOptions,
            Group partitionInfo,
            Operator aggregationInfo) {
        Arguments.requireNonNull(origin);
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(sourceType);
        Arguments.requireNonNull(wireType);
        Arguments.requireNonNull(outputType);
        Arguments.requireNonNull(outputOptions);
        this.origin = origin;
        this.id = id;
        this.sourceType = sourceType;
        this.dataType = wireType;
        this.outputType = outputType;
        this.outputOptions = EnumUtil.freeze(outputOptions);
        this.partitionInfo = partitionInfo;
        this.aggregationInfo = aggregationInfo;
    }

    @Override
    public SubPlan.Output getOrigin() {
        return origin;
    }

    @Override
    public String getId() {
        return id;
    }

    /**
     * Returns the source type.
     * @return the source type
     */
    public TypeDescription getSourceType() {
        return sourceType;
    }

    /**
     * Returns the output data type (on the wire).
     * @return the data type
     */
    public TypeDescription getDataType() {
        return dataType;
    }

    /**
     * Returns the output operation type.
     * @return the operation type
     */
    public OutputType getOutputType() {
        return outputType;
    }

    /**
     * Returns the extra output options.
     * @return the output options
     */
    public Set<OutputOption> getOutputOptions() {
        return outputOptions;
    }

    /**
     * Returns the partitioning information.
     * @return the partitioning information, or {@code null} if it is not defined
     */
    public Group getPartitionInfo() {
        return partitionInfo;
    }

    /**
     * Returns the output pre-aggregation operator.
     * @return the output pre-aggregation operator, or {@code null} if it is not defined
     */
    public Operator getAggregationInfo() {
        return aggregationInfo;
    }

    @Override
    public Map<String, ?> toMap() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("id", getId()); //$NON-NLS-1$
        results.put("type", getOutputType()); //$NON-NLS-1$
        results.put("data", getDataType()); //$NON-NLS-1$
        results.put("options", getOutputOptions()); //$NON-NLS-1$
        Optionals.put(results, "partition", getPartitionInfo()); //$NON-NLS-1$
        Optionals.put(results, "aggregation", getAggregationInfo()); //$NON-NLS-1$
        return results;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }

    /**
     * Represents an output operation type.
     * @since 0.4.0
     */
    public enum OutputType {

        /**
         * No succeeding operations.
         */
        DISCARD,

        /**
         * Output edge accepts broadcast data.
         */
        BROADCAST,

        /**
         * Output edge accepts values.
         */
        VALUE,

        /**
         * Output edge accepts key-value pairs.
         */
        KEY_VALUE,
    }

    /**
     * Represents extra attributes for outputs.
     * @since 0.4.0
     */
    public enum OutputOption {

        /**
         * Whether the output accepts pre-aggregation operation.
         */
        PRE_AGGREGATION,
    }
}
