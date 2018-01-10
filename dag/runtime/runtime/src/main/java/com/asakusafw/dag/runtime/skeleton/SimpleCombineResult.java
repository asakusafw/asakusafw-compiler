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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;

import com.asakusafw.dag.runtime.adapter.CoGroupOperation;
import com.asakusafw.runtime.core.Result;

/**
 * An adapter implementation of {@link CoGroupOperation} for simplified aggregation operations.
 * @param <T> the combining type
 * @since 0.4.1
 */
public abstract class SimpleCombineResult<T> implements Result<CoGroupOperation.Input> {

    @Override
    public void add(CoGroupOperation.Input result) {
        try {
            CoGroupOperation.Cursor<T> group = result.getCursor(0);
            if (group.nextObject() == false) {
                return;
            }
            start(group.getObject());
            while (group.nextObject()) {
                combine(group.getObject());
            }
            finish();
        } catch (IOException | InterruptedException e) {
            throw new OutputException(e);
        }
    }

    /**
     * Starts a group and processes the first element of the group.
     * @param object the first element
     */
    protected abstract void start(T object);

    /**
     * Processes a rest element of the current group.
     * @param object the target element
     */
    protected abstract void combine(T object);

    /**
     * Finishes the processing of the current group.
     */
    protected abstract void finish();
}
