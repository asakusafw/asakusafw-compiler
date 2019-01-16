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
package com.asakusafw.lang.compiler.hadoop;

import java.util.Collection;
import java.util.Collections;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An extension for using Hadoop tasks.
 * @since 0.1.0
 * @version 0.3.0
 */
@FunctionalInterface
public interface HadoopTaskExtension {

    /**
     * Adds a Hadoop sub-application to execute in this application.
     * @param phase the execution phase
     * @param mainClass the main class
     * @param blockers the blocker sub-applications
     * @return a symbol that represents the added sub-application
     */
    default TaskReference addTask(TaskReference.Phase phase, ClassDescription mainClass, TaskReference... blockers) {
        return addTask(phase, mainClass, Collections.emptySet(), blockers);
    }

    /**
     * Adds a Hadoop sub-application to execute in this application.
     * @param phase the execution phase
     * @param mainClass the main class
     * @param extensions the acceptable extension names
     * @param blockers the blocker sub-applications
     * @return a symbol that represents the added sub-application
     * @since 0.3.0
     */
    TaskReference addTask(
            TaskReference.Phase phase,
            ClassDescription mainClass,
            Collection<String> extensions,
            TaskReference... blockers);
}
