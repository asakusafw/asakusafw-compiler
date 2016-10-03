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
package com.asakusafw.dag.runtime.skeleton;

import static com.asakusafw.dag.runtime.skeleton.CoGroupOperationUtil.*;

import java.io.IOException;
import java.util.List;

import com.asakusafw.dag.runtime.adapter.CoGroupOperation;
import com.asakusafw.runtime.core.Result;

/**
 * An adapter implementation of {@link CoGroupOperation} for merge-join operations.
 * @param <TMaster> the master object type
 * @param <TTransaction> the transaction object type
 * @since 0.4.0
 */
public abstract class MergeJoinResult<TMaster, TTransaction> implements Result<CoGroupOperation.Input> {

    private final int indexMaster;

    private final int indexTransaction;

    /**
     * Creates a new instance.
     * @param indexMaster the group index of the master input
     * @param indexTransaction the group index of the transaction input
     */
    public MergeJoinResult(int indexMaster, int indexTransaction) {
        this.indexMaster = indexMaster;
        this.indexTransaction = indexTransaction;
    }

    @Override
    public void add(CoGroupOperation.Input result) {
        try {
            List<TMaster> masterCandidates = getList(result, indexMaster);
            CoGroupOperation.Cursor<TTransaction> transactions = getCursor(result, indexTransaction);
            while (transactions.nextObject()) {
                TTransaction transaction = transactions.getObject();
                TMaster master = selectMaster(masterCandidates, transaction);
                process(master, transaction);
            }
        } catch (IOException | InterruptedException e) {
            throw new OutputException(e);
        }
    }

    /**
     * Selects a master object from the candidates, and returns it.
     * @param masterCandidates the master object candidates
     * @param transaction the transaction object
     * @return the selected master object, or {@code null} if there is no suitable master object
     */
    protected TMaster selectMaster(List<TMaster> masterCandidates, TTransaction transaction) {
        if (masterCandidates.isEmpty()) {
            return null;
        }
        return masterCandidates.get(0);
    }

    /**
     * Processes the joining pair.
     * @param master the master object, or {@code null} if there is no suitable master object
     * @param transaction the transaction object
     */
    protected abstract void process(TMaster master, TTransaction transaction);
}
