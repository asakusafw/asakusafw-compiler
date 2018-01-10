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
package com.asakusafw.lang.compiler.optimizer;

import java.util.Collection;
import java.util.Map;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;

/**
 * Estimates statistics of each {@link Operator}.
 */
@FunctionalInterface
public interface OperatorEstimator {

    /**
     * Null implementation of {@link OperatorEstimator}.
     */
    OperatorEstimator NULL = (context, operator) -> {
        return;
    };

    /**
     * Performs to estimate statistics of the target operator.
     * @param context the current context
     * @param operator the target operator
     * @throws DiagnosticException if failed to estimate about the target operator
     */
    void perform(Context context, Operator operator);

    /**
     * Represents a context object for {@link OperatorEstimator}.
     */
    public interface Context extends OptimizerContext {

        /**
         * Sets the estimated output size of the current target operator.
         * @param port the estimated output size in bytes, which the current target operator must have it
         * @param size the estimated size
         */
        void putSize(OperatorOutput port, double size);

        /**
         * Sets an attribute to the target operator.
         * @param <T> the attribute type
         * @param attributeType the attribute type
         * @param attributeValue the attribute value
         */
        <T> void putAttribute(Class<T> attributeType, T attributeValue);

        /**
         * Sets an attribute to the target input port.
         * @param <T> the attribute type
         * @param port the target port
         * @param attributeType the attribute type
         * @param attributeValue the attribute value
         */
        <T> void putAttribute(OperatorInput port, Class<T> attributeType, T attributeValue);

        /**
         * Sets an attribute to the target input port.
         * @param <T> the attribute type
         * @param port the target port
         * @param attributeType the attribute type
         * @param attributeValue the attribute value
         */
        <T> void putAttribute(OperatorOutput port, Class<T> attributeType, T attributeValue);

        /**
         * Estimates statistics for the target operator.
         * If the target operator is not a predecessor of the current estimating one,
         * this always returns {@link OperatorEstimate#UNKNOWN unknown}.
         * @param operator the target operator
         * @return the estimate for the operator
         * @throws DiagnosticException if failed to estimate statistics for the target operator
         */
        OperatorEstimate estimate(Operator operator);

        /**
         * Estimates statistics for the target operators.
         * If the target operator is not a predecessor of the current estimating one,
         * this always returns {@link OperatorEstimate#UNKNOWN unknown}.
         * @param operators the target operators
         * @return the estimate for each operator
         * @throws DiagnosticException if failed to estimate statistics for the target operators
         */
        Map<Operator, OperatorEstimate> estimate(Operator... operators);

        /**
         * Estimates statistics for the target operators.
         * If the target operator is not a predecessor of the current estimating one,
         * this always returns {@link OperatorEstimate#UNKNOWN unknown}.
         * @param operators the target operators
         * @return the estimate for each operator
         * @throws DiagnosticException if failed to estimate statistics for the target operators
         */
        Map<Operator, OperatorEstimate> estimate(Collection<? extends Operator> operators);

        /**
         * Applies this context to the target estimator.
         * Clients should not use this method directly.
         * @param estimator the target estimator
         * @param operators the target operators
         */
        void apply(OperatorEstimator estimator, Collection<? extends Operator> operators);
    }
}
