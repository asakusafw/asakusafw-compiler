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
package com.asakusafw.dag.runtime.io;

import java.io.IOException;
import java.util.function.LongConsumer;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * A {@link ModelOutput} with counting the number of objects.
 * @param <T> the data type
 * @since 0.4.0
 */
public class CountingModelOutput<T> implements ModelOutput<T> {

    private final ModelOutput<T> delegate;

    private final LongConsumer counter;

    private long count;

    /**
     * Creates a new instance.
     * @param delegate the target {@link ModelOutput}
     * @param counter the count sink
     */
    public CountingModelOutput(ModelOutput<T> delegate, LongConsumer counter) {
        Arguments.requireNonNull(delegate);
        Arguments.requireNonNull(counter);
        this.delegate = delegate;
        this.counter = counter;
    }

    @Override
    public void write(T model) throws IOException {
        delegate.write(model);
        count++;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        counter.accept(count);
        count = 0;
    }
}
