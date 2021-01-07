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
package com.asakusafw.dag.runtime.skeleton;

import java.util.List;

import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.dag.runtime.adapter.KeyExtractor;
import com.asakusafw.runtime.core.Result;

/**
 * An adapter implementation of table-join operations.
 * @param <TMaster> the master object type
 * @param <TTransaction> the transaction object type
 * @since 0.4.0
 */
public abstract class TableJoinResult<TMaster, TTransaction>
        implements KeyExtractor<TTransaction>, Result<TTransaction> {

    private final DataTable<TMaster> dataTable;

    private final KeyBuffer keyBuffer;

    /**
     * Creates a new instance.
     * @param dataTable the data table of master input
     */
    public TableJoinResult(DataTable<TMaster> dataTable) {
        this.dataTable = dataTable;
        this.keyBuffer = dataTable.newKeyBuffer();
    }

    @Override
    public void add(TTransaction transaction) {
        KeyBuffer kb = keyBuffer;
        kb.clear();
        buildKey(kb, transaction);
        List<TMaster> masterCandidates = dataTable.getList(kb);
        TMaster master = selectMaster(masterCandidates, transaction);
        process(master, transaction);
    }

    /**
     * Builds the key contents for looking up the set of corresponded master data.
     * @param target the target key buffer without any contents
     * @param transaction the transaction object
     */
    @Override
    public abstract void buildKey(KeyBuffer target, TTransaction transaction);

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
