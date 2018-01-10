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
import com.asakusafw.lang.compiler.optimizer.adapter.OperatorEstimatorAdapter;

/**
 * Utilities for {@link OperatorEstimator}.
 */
public final class OperatorEstimators {

    private OperatorEstimators() {
        return;
    }

    /**
     * Returns the estimated input size.
     * @param context the current context
     * @param port the target port
     * @return the estimated size in bytes
     */
    public static double getSize(OperatorEstimator.Context context, OperatorInput port) {
        double cached = getSizeCached(context, port);
        if (Double.isNaN(cached) == false) {
            return cached;
        }
        double total = 0.0;
        for (OperatorOutput upstream : port.getOpposites()) {
            double size = getSize(context, upstream);
            if (Double.isNaN(size)) {
                return OperatorEstimate.UNKNOWN_SIZE;
            }
            total += size;
        }
        return total;
    }

    /**
     * Returns the estimated output size.
     * @param context the current context
     * @param port the target port
     * @return the estimated size in bytes
     */
    public static double getSize(OperatorEstimator.Context context, OperatorOutput port) {
        OperatorEstimate estimate = context.estimate(port.getOwner());
        return estimate.getSize(port);
    }

    /**
     * Sets the estimated size to each operator output.
     * @param context the current context
     * @param operator the target operators
     * @param size the estimated size in bytes
     */
    public static void putSize(OperatorEstimator.Context context, Operator operator, double size) {
        for (OperatorOutput output : operator.getOutputs()) {
            context.putSize(output, size);
        }
    }

    private static double getSizeCached(OperatorEstimator.Context context, OperatorInput port) {
        OperatorEstimate estimate = context.estimate(port.getOwner());
        double size = estimate.getSize(port);
        return size;
    }

    /**
     * Applies the {@link OperatorEstimator}.
     * @param context the current context
     * @param estimator the target estimator
     * @param operators the target operators
     * @return the estimated results for each operator
     * @throws DiagnosticException if failed to estimate about operators
     */
    public static Map<Operator, OperatorEstimate> apply(
            OptimizerContext context,
            OperatorEstimator estimator,
            Collection<? extends Operator> operators) {
        return apply(new OperatorEstimatorAdapter(context), estimator, operators);
    }

    /**
     * Applies the {@link OperatorEstimator}.
     * @param context the current context
     * @param estimator the target estimator
     * @param operators the target operators
     * @return the estimated results for each operator
     * @throws DiagnosticException if failed to estimate about operators
     */
    public static Map<Operator, OperatorEstimate> apply(
            OperatorEstimator.Context context,
            OperatorEstimator estimator,
            Collection<? extends Operator> operators) {
        context.apply(estimator, operators);
        return context.estimate(operators);
    }
}
