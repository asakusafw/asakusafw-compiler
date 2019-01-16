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
package com.asakusafw.dag.api.processor;

import java.io.IOException;
import java.util.Optional;

import com.asakusafw.lang.utils.common.Optionals;

/**
 * Processes vertex operations on Asakusa DAG.
 * @since 0.4.0
 */
@FunctionalInterface
public interface VertexProcessor extends Processor {

    /**
     * Initializes this processor.
     * Clients can capture {@link VertexProcessorContext} by overriding this method.
     * This can return custom {@link TaskSchedule} only if this vertex does NOT have any
     * non-broadcast inputs.
     * @param context the current context
     * @return the optional task schedule
     * @throws IOException if I/O error was occurred while initializing this object
     * @throws InterruptedException if interrupted while initializing this object
     */
    default Optional<? extends TaskSchedule> initialize(
            VertexProcessorContext context) throws IOException, InterruptedException {
        return Optionals.empty();
    }

    /**
     * Returns the number of max concurrency.
     * @return the number of max concurrency, or {@code -1} if it is not defined
     */
    default int getMaxConcurrency() {
        return -1;
    }

    /**
     * Creates a thread-local task processor for the current thread.
     * @return the created processor
     * @throws IOException if I/O error was occurred while creating a new processor
     * @throws InterruptedException if interrupted while creating a new processor
     */
    TaskProcessor createTaskProcessor() throws IOException, InterruptedException;
}
