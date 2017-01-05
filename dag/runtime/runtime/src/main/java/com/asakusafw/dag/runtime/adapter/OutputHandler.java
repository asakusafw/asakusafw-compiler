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
package com.asakusafw.dag.runtime.adapter;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.runtime.core.Result;

/**
 * An abstract super interface of vertex/task output handlers.
 * @param <C> the context type
 * @since 0.4.0
 */
public interface OutputHandler<C extends ProcessorContext> extends ContextHandler<C> {

    /**
     * Returns whether this can handle the target ID.
     * @param id the target output ID
     * @return {@code true} if this can handle the target ID, otherwise {@code false}
     */
    boolean contains(String id);

    /**
     * Returns the result sink for the specified ID.
     * Clients must invoke {@link #start(ProcessorContext)} before adding objects to the sink.
     * @param <T> the object type
     * @param type the object type
     * @param id the output ID
     * @return {@link Result} object for output
     */
    <T> Result<T> getSink(Class<T> type, String id);
}
