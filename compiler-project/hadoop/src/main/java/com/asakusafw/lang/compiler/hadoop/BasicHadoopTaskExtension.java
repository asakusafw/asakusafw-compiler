/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.hadoop;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.asakusafw.lang.compiler.api.basic.TaskContainer;
import com.asakusafw.lang.compiler.api.basic.TaskContainerMap;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Basic implementation of {@link HadoopTaskExtension}.
 */
public class BasicHadoopTaskExtension implements HadoopTaskExtension {

    private final TaskContainerMap tasks;

    /**
     * Creates a new instance.
     * @param tasks the task reference sink
     */
    public BasicHadoopTaskExtension(TaskContainerMap tasks) {
        this.tasks = tasks;
    }

    @Override
    public TaskReference addTask(Phase phase, ClassDescription mainClass, TaskReference... blockers) {
        return addTask(phase, mainClass, Collections.emptySet(), blockers);
    }

    @Override
    public TaskReference addTask(
            Phase phase,
            ClassDescription mainClass,
            Collection<String> extensions,
            TaskReference... blockers) {
        HadoopTaskReference task = new HadoopTaskReference(mainClass, extensions, Arrays.asList(blockers));
        TaskContainer container = tasks.getTaskContainer(phase);
        container.add(task);
        return task;
    }
}
