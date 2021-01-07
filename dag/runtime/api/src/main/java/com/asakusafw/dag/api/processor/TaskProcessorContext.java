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
package com.asakusafw.dag.api.processor;

import java.util.Optional;

import com.asakusafw.lang.utils.common.Optionals;

/**
 * A context object for processing tasks.
 * @since 0.4.0
 * @see TaskProcessor
 */
public interface TaskProcessorContext extends EdgeIoProcessorContext {

    /**
     * Returns the current vertex ID.
     * @return the current vertex ID
     */
    String getVertexId();

    /**
     * Returns the local ID of this task.
     * Each task ID must unique in the running vertex.
     * @return the local task ID
     */
    String getTaskId();

    /**
     * Returns the custom information of the current task.
     * @return the custom task information
     */
    default Optional<TaskInfo> getTaskInfo() {
        return Optionals.empty();
    }
}
