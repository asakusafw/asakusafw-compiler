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
package com.asakusafw.dag.runtime.directio;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.dag.api.counter.CounterGroup;
import com.asakusafw.dag.api.counter.basic.BasicCounterGroupCategory;
import com.asakusafw.dag.api.counter.basic.StandardColumn;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.runtime.directio.Counter;

/**
 * An implementation of {@link CounterGroup} for Direct I/O.
 * @since 0.4.0
 */
public class DirectFileCounterGroup implements CounterGroup {

    /**
     * The {@link CounterGroup} category for Direct I/O file inputs.
     */
    public static final Category<DirectFileCounterGroup> CATEGORY_INPUT = new BasicCounterGroupCategory<>(
            "Direct I/O file input",
            Scope.GRAPH,
            Arrays.asList(StandardColumn.INPUT_FILE_SIZE, StandardColumn.INPUT_RECORD),
            "directio-0-input", //$NON-NLS-1$
            () -> new DirectFileCounterGroup(StandardColumn.INPUT_FILE_SIZE, StandardColumn.INPUT_RECORD));

    /**
     * The {@link CounterGroup} category for Direct I/O file outputs.
     */
    public static final Category<DirectFileCounterGroup> CATEGORY_OUTPUT = new BasicCounterGroupCategory<>(
            "Direct I/O file output",
            Scope.GRAPH,
            Arrays.asList(StandardColumn.OUTPUT_FILE_SIZE, StandardColumn.OUTPUT_RECORD),
            "directio-1-output", //$NON-NLS-1$
            () -> new DirectFileCounterGroup(StandardColumn.OUTPUT_FILE_SIZE, StandardColumn.OUTPUT_RECORD));

    private final Map<Column, Counter> counters = new LinkedHashMap<>();

    private final Counter fileSize = new Counter();

    private final Counter recordCount = new Counter();

    /**
     * Creates a new instance.
     * @param fileSize the file size in bytes
     * @param recordCount the record count
     */
    public DirectFileCounterGroup(Column fileSize, Column recordCount) {
        Arguments.requireNonNull(fileSize);
        Arguments.requireNonNull(recordCount);
        counters.put(fileSize, this.fileSize);
        counters.put(recordCount, this.recordCount);
    }

    @Override
    public long getCount(Column column) {
        Counter counter = counters.get(column);
        Invariants.requireNonNull(counter);
        return counter.get();
    }

    /**
     * Returns the file size.
     * @return the file size
     */
    public Counter getFileSize() {
        return fileSize;
    }

    /**
     * Returns the record count.
     * @return the record count
     */
    public Counter getRecordCount() {
        return recordCount;
    }
}
