/**
 * Copyright 2011-2019 Asakusa Framework Team.
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

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.analyzer.util.MasterJoinOperatorUtil;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.lang.compiler.optimizer.OptimizerToolkit;

/**
 * Removes empty {@code master-join} like operators.
 * @since 0.3.1
 */
public class EmptyMasterJoinRemover implements OperatorRewriter {

    static final Logger LOG = LoggerFactory.getLogger(EmptyMasterJoinRemover.class);

    /**
     * The compiler option key of whether this removes master-join like operators with empty master port.
     */
    public static final String KEY_REMOVE_EMPTY_MASTER = "operator.masterjoin.remove.empty.master";

    /**
     * The compiler option key of whether this removes master-join like operators with empty transaction port.
     */
    public static final String KEY_REMOVE_EMPTY_TRANACTION = "operator.masterjoin.remove.empty.transaction";

    static final boolean DEFAULT_REMOVE_EMPTY_MASTER = false;

    static final boolean DEFAULT_REMOVE_EMPTY_TRANSACTION = false;

    @Override
    public void perform(Context context, OperatorGraph graph) {
        boolean changed = false;
        if (context.getOptions().get(KEY_REMOVE_EMPTY_MASTER, DEFAULT_REMOVE_EMPTY_MASTER)) {
            LOG.debug("applying empty join remover: flow={}, port=master", context.getFlowId());
            for (Operator operator : graph.getOperators()) {
                if (MasterJoinOperatorUtil.isSupported(operator)) {
                    boolean removed = removeEmptyJoinMaster(context.getToolkit(), operator);
                    if (removed) {
                        graph.remove(operator);
                        changed = true;
                    }
                }
            }
        }
        if (context.getOptions().get(KEY_REMOVE_EMPTY_TRANACTION, DEFAULT_REMOVE_EMPTY_TRANSACTION)) {
            LOG.debug("applying empty join remover: flow={}, port=transaction", context.getFlowId());
            for (Operator operator : graph.getOperators()) {
                boolean removed = removeEmptyJoinTransaction(context.getToolkit(), operator);
                if (removed) {
                    graph.remove(operator);
                    changed = true;
                }
            }
        }
        if (changed) {
            context.getToolkit().repair(graph);
        }
    }

    private boolean removeEmptyJoinMaster(OptimizerToolkit toolkit, Operator operator) {
        if (MasterJoinOperatorUtil.isSupported(operator) == false) {
            return false;
        }
        if (toolkit.hasEffectiveOpposites(MasterJoinOperatorUtil.getMasterInput(operator))) {
            return false;
        }
        if (MasterJoinOperatorUtil.hasSelection(operator)) {
            return false;
        }
        OperatorOutput destination = MasterJoinOperatorUtil.getNotJoinedOutput(operator);
        if (destination == null) {
            return false;
        }
        LOG.debug("removing empty join: {}", operator);
        OperatorInput transaction = MasterJoinOperatorUtil.getTransactionInput(operator);
        Collection<OperatorOutput> upstreams = transaction.getOpposites();
        Collection<OperatorInput> downstreams = destination.getOpposites();
        operator.disconnectAll();
        Operators.connectAll(upstreams, downstreams);
        return true;
    }

    private boolean removeEmptyJoinTransaction(OptimizerToolkit toolkit, Operator operator) {
        if (MasterJoinOperatorUtil.isSupported(operator) == false) {
            return false;
        }
        if (toolkit.hasEffectiveOpposites(MasterJoinOperatorUtil.getTransactionInput(operator))) {
            return false;
        }
        LOG.debug("removing empty join: {}", operator);
        operator.disconnectAll();
        return true;
    }
}
