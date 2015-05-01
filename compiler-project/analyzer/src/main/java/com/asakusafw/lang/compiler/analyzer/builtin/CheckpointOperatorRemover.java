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
package com.asakusafw.lang.compiler.analyzer.builtin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.vocabulary.operator.Checkpoint;

/**
 * Removes {@link Checkpoint} operators.
 */
public class CheckpointOperatorRemover implements OperatorRewriter {

    static final Logger LOG = LoggerFactory.getLogger(CheckpointOperatorRemover.class);

    /**
     * The compiler option key of whether this rewriter is enabled or not.
     */
    public static final String KEY_ENABLE = "operator.checkpoint.remove"; //$NON-NLS-1$

    /**
     * The default value of {@value #KEY_ENABLE}.
     */
    public static final boolean DEFAULT_ENABLE = false;

    @Override
    public void perform(Context context, OperatorGraph graph) {
        if (isEnabled(context.getOptions()) == false) {
            return;
        }
        LOG.debug("applying checkpoint operator remover: flow={}", context.getFlowId()); //$NON-NLS-1$
        for (Operator operator : graph.getOperators(true)) {
            if (isTarget(operator)) {
                LOG.debug("removing checkpoint operator: operator={}", operator); //$NON-NLS-1$
                Operators.remove(operator);
                graph.remove(operator);
            }
        }
    }

    private static boolean isEnabled(CompilerOptions options) {
        return options.get(KEY_ENABLE, DEFAULT_ENABLE);
    }

    private boolean isTarget(Operator operator) {
        if (operator.getOperatorKind() != OperatorKind.CORE) {
            return false;
        }
        return ((CoreOperator) operator).getCoreOperatorKind() == CoreOperatorKind.CHECKPOINT;
    }
}
