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
package com.asakusafw.lang.compiler.api.testing;

import com.asakusafw.lang.compiler.api.basic.TaskContainer;
import com.asakusafw.lang.compiler.api.basic.TaskContainerMap;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;

/**
 * Mock implementation of {@link TaskReferenceMap}.
 */
public class MockTaskReferenceMap extends TaskContainerMap {

    /**
     * Adds a {@link TaskReference}.
     * @param phase the target phase
     * @param task the target task
     * @return this
     */
    public MockTaskReferenceMap add(TaskReference.Phase phase, TaskReference task) {
        getTaskContainer(phase).add(task);
        return this;
    }

    /**
     * Adds {@link TaskReference}s.
     * @param phase the target phase
     * @param tasks target tasks
     * @return this
     */
    public MockTaskReferenceMap add(TaskReference.Phase phase, TaskReference... tasks) {
        TaskContainer container = getTaskContainer(phase);
        for (TaskReference task : tasks) {
            container.add(task);
        }
        return this;
    }
}
