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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * A shared buffer.
 * @since 0.4.0
 */
public final class SharedBuffer implements DataReader.Provider {

    private final DataReader.Provider origin;

    private final AtomicInteger latch;

    private final AtomicBoolean closed = new AtomicBoolean();

    private SharedBuffer(DataReader.Provider origin, AtomicInteger latch) {
        this.origin = origin;
        this.latch = latch;
    }

    /**
     * Wraps the given provider with the given count.
     * @param origin the original provider
     * @param count the sharing count
     * @return the wrapped instance
     */
    public static List<DataReader.Provider> wrap(DataReader.Provider origin, int count) {
        Arguments.requireNonNull(origin);
        Arguments.require(count >= 1);
        if (count == 1) {
            return Collections.singletonList(origin);
        }
        AtomicInteger latch = new AtomicInteger(count);
        List<DataReader.Provider> results = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            results.add(new SharedBuffer(origin, latch));
        }
        return results;
    }

    @Override
    public DataReader open() throws IOException, InterruptedException {
        Invariants.require(closed.get() == false);
        return origin.open();
    }

    @Override
    public void close() throws IOException, InterruptedException {
        if (closed.compareAndSet(false, true)) {
            if (latch.decrementAndGet() <= 0) {
                origin.close();
            }
        }
    }
}
