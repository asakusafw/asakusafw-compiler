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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.runtime.adapter.CoGroupOperation;
import com.asakusafw.dag.runtime.adapter.ObjectCombiner;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.model.DataModel;

/**
 * An adapter implementation of {@link CoGroupOperation} for aggregate operations.
 * @param <T> the combining type
 * @since 0.4.0
 */
public class CombineResult<T extends DataModel<T>> implements Result<CoGroupOperation.Input> {

    static final Logger LOG = LoggerFactory.getLogger(CombineResult.class);

    private final ObjectCombiner<T> combiner;

    private final Result<? super T> next;

    private final T buffer;

    /**
     * Creates a new instance.
     * @param combiner the combiner
     * @param buffer the data buffer
     * @param next the next operation
     */
    public CombineResult(ObjectCombiner<T> combiner, T buffer, Result<? super T> next) {
        Arguments.requireNonNull(combiner);
        Arguments.requireNonNull(next);
        this.combiner = combiner;
        this.next = next;
        this.buffer = buffer;
    }

    @Override
    public void add(CoGroupOperation.Input result) {
        T current;
        try {
            CoGroupOperation.Cursor<T> group = result.getCursor(0);
            if (group.nextObject() == false) {
                return;
            }
            current = group.getObject();
            buffer.copyFrom(current);
            current = buffer;
            if (group.nextObject()) {
                ObjectCombiner<T> f = combiner;
                do {
                    T right = group.getObject();
                    f.combine(current, right);
                } while (group.nextObject());
            }
        } catch (IOException | InterruptedException e) {
            throw new OutputException(e);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("combine result: {}", current);
        }
        next.add(current);
    }
}
