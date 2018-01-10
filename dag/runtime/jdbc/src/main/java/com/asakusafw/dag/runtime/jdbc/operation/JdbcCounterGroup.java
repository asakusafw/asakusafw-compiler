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
package com.asakusafw.dag.runtime.jdbc.operation;

import java.util.Collections;
import java.util.concurrent.atomic.LongAdder;

import com.asakusafw.dag.api.counter.CounterGroup;
import com.asakusafw.dag.api.counter.basic.AbstractCounterGroup;
import com.asakusafw.dag.api.counter.basic.BasicCounterGroupCategory;
import com.asakusafw.dag.api.counter.basic.StandardColumn;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An implementation of {@link CounterGroup} for JDBC.
 * @since 0.4.0
 */
public final class JdbcCounterGroup extends AbstractCounterGroup {

    /**
     * The {@link CounterGroup} category for JDBC inputs.
     */
    public static final Category<JdbcCounterGroup> CATEGORY_INPUT = new BasicCounterGroupCategory<>(
            "JDBC input",
            Scope.GRAPH,
            Collections.singletonList(StandardColumn.INPUT_RECORD),
            "jdbc-0-input", //$NON-NLS-1$
            () -> new JdbcCounterGroup(StandardColumn.INPUT_RECORD));

    /**
     * The {@link CounterGroup} category for JDBC outputs.
     */
    public static final Category<JdbcCounterGroup> CATEGORY_OUTPUT = new BasicCounterGroupCategory<>(
            "JDBC output",
            Scope.GRAPH,
            Collections.singletonList(StandardColumn.OUTPUT_RECORD),
            "jdbc-1-output", //$NON-NLS-1$
            () -> new JdbcCounterGroup(StandardColumn.OUTPUT_RECORD));

    private final LongAdder counter;

    JdbcCounterGroup(Column column) {
        Arguments.requireNonNull(column);
        this.counter = register(column);
    }

    /**
     * Increments the counter.
     * @param numberOfRecords the number of records
     */
    public void add(long numberOfRecords) {
        this.counter.add(numberOfRecords);
    }
}
