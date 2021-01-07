/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;

/**
 * Rewrites {@link OperatorGraph} objects.
 */
@FunctionalInterface
public interface OperatorRewriter {

    /**
     * Null implementation of {@link OperatorRewriter}.
     */
    OperatorRewriter NULL = (context, graph) -> {
        return;
    };

    /**
     * Performs this rewriter.
     * @param context the current context
     * @param graph the target operator graph (flattened)
     * @throws DiagnosticException if failed to rewrite the target operator graph
     */
    void perform(Context context, OperatorGraph graph);

    /**
     * Represents a context object for {@link OperatorRewriter}.
     */
    public interface Context extends OptimizerContext {

        /**
         * Estimates statistics for the target operator.
         * @param operator the target operator
         * @return the estimate for the operator
         * @throws DiagnosticException if failed to estimate statistics for the target operator
         */
        OperatorEstimate estimate(Operator operator);

        /**
         * Estimates statistics for the target operators.
         * @param operators the target operators
         * @return the estimate for each operator
         * @throws DiagnosticException if failed to estimate statistics for the target operators
         */
        Map<Operator, OperatorEstimate> estimate(Operator... operators);

        /**
         * Estimates statistics for the target operators.
         * @param operators the target operators
         * @return the estimate for each operator
         * @throws DiagnosticException if failed to estimate statistics for the target operators
         */
        Map<Operator, OperatorEstimate> estimate(Collection<? extends Operator> operators);
    }
}
