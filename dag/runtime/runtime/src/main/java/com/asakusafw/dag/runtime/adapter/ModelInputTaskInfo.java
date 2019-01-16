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
package com.asakusafw.dag.runtime.adapter;

import java.io.IOException;

import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.runtime.io.ModelInput;

/**
 * A {@link TaskInfo} which can provide {@link ModelInput}.
 * @param <T> the input data type
 * @since 0.4.0
 */
public interface ModelInputTaskInfo<T> extends TaskInfo {

    /**
     * Opens the task input.
     * @return the opened input
     * @throws IOException if I/O error was occurred while opening input
     * @throws InterruptedException if interrupted while opening input
     */
    ModelInput<T> open() throws IOException, InterruptedException;

    /**
     * Returns a new data object.
     * @return the created object
     */
    T newDataObject();
}
