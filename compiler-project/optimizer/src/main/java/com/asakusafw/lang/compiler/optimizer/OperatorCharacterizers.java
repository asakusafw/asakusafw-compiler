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
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.optimizer.adapter.OperatorCharacterizerAdapter;
import com.asakusafw.lang.compiler.optimizer.adapter.OperatorEstimatorAdapter;

/**
 * Utilities for {@link OperatorCharacterizer}.
 */
public final class OperatorCharacterizers {

    private OperatorCharacterizers() {
        return;
    }

    /**
     * Creates a new context for {@link OperatorCharacterizer}.
     * @param context the current optimizer context
     * @param estimator the estimator for characterization
     * @return the created context
     */
    public static OperatorCharacterizer.Context newContext(OptimizerContext context, OperatorEstimator estimator) {
        return new OperatorCharacterizerAdapter(
                new OperatorEstimatorAdapter(context),
                estimator);
    }

    /**
     * Applies the {@link OperatorCharacterizer}.
     * @param <T> the characteristics type
     * @param context the current context
     * @param estimator the operator estimator
     * @param characterizer the target characterizer
     * @param operators the target operators
     * @return characteristics information for each operator
     * @throws DiagnosticException if failed to characterize about operators
     */
    public static <T extends OperatorCharacteristics> Map<Operator, T> apply(
            OptimizerContext context,
            OperatorEstimator estimator,
            OperatorCharacterizer<? extends T> characterizer,
            Collection<? extends Operator> operators) {
        OperatorCharacterizer.Context adapter = newContext(context, estimator);
        return apply(adapter, characterizer, operators);
    }

    /**
     * Applies the {@link OperatorCharacterizer}.
     * @param <T> the characteristics type
     * @param context the current context
     * @param characterizer the target characterizer
     * @param operators the target operators
     * @return characteristics information for each operator
     * @throws DiagnosticException if failed to characterize about operators
     */
    public static <T extends OperatorCharacteristics> Map<Operator, T> apply(
            OperatorCharacterizer.Context context,
            OperatorCharacterizer<? extends T> characterizer,
            Collection<? extends Operator> operators) {
        Map<Operator, T> results = new LinkedHashMap<>();
        for (Operator operator : operators) {
            T result = characterizer.extract(context, operator);
            results.put(operator, result);
        }
        return results;
    }
}
