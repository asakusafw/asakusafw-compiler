/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.api.counter.basic;

import java.util.concurrent.atomic.LongAdder;

import com.asakusafw.dag.api.counter.CounterGroup;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * A {@link CounterGroup} for record I/O edge.
 * @since 0.4.0
 */
public class ValueEdgeCounterGroup extends AbstractCounterGroup {

    private final LongAdder dataSize;

    private final LongAdder recordCount;

    /**
     * Creates a new instance.
     * @param dataSize the data size column
     * @param recordCount the record count column
     */
    public ValueEdgeCounterGroup(CounterGroup.Column dataSize, CounterGroup.Column recordCount) {
        Arguments.requireNonNull(dataSize);
        Arguments.requireNonNull(recordCount);
        this.dataSize = register(dataSize);
        this.recordCount = register(recordCount);
    }

    /**
     * Adds the data size.
     * @param count the data size in bytes
     */
    public final void addDataSize(long count) {
        dataSize.add(count);
    }

    /**
     * Adds the record count.
     * @param count the number of records
     */
    public final void addRecordCount(long count) {
        recordCount.add(count);
    }
}
