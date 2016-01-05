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
package com.asakusafw.lang.compiler.optimizer;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.optimizer.adapter.OperatorRewriterAdapter;
import com.asakusafw.lang.compiler.optimizer.basic.CompositeOperatorRewriter;

/**
 * Utilities for {@link OperatorRewriter}.
 */
public final class OperatorRewriters {

    private OperatorRewriters() {
        return;
    }

    /**
     * Creates a new context.
     * @param context the current optimizer context
     * @param estimator the estimator
     * @return the created context
     */
    private static OperatorRewriter.Context newContext(OptimizerContext context, OperatorEstimator estimator) {
        return new OperatorRewriterAdapter(
                context,
                estimator == null ? OperatorEstimator.NULL : estimator);
    }

    /**
     * Applies the {@link OperatorRewriter}.
     * @param context the current context
     * @param estimator the estimation engine
     * @param rewriter the target rewriter
     * @param graph the target operator graph
     * @throws DiagnosticException if failed to rewrite the target operator graph
     * @see CompositeOperatorRewriter
     */
    public static void apply(
            OptimizerContext context,
            OperatorEstimator estimator,
            OperatorRewriter rewriter,
            OperatorGraph graph) {
        if (rewriter == OperatorRewriter.NULL) {
            return;
        }
        OperatorRewriter.Context adapter = newContext(context, estimator);
        apply(adapter, rewriter, graph);
    }

    /**
     * Applies the {@link OperatorRewriter}.
     * @param context the current context
     * @param rewriter the target rewriter
     * @param graph the target operator graph
     * @throws DiagnosticException if failed to rewrite the target operator graph
     * @see CompositeOperatorRewriter
     */
    private static void apply(
            OperatorRewriter.Context context,
            OperatorRewriter rewriter,
            OperatorGraph graph) {
        rewriter.perform(context, graph);
    }
}
