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
package com.asakusafw.lang.compiler.api.basic;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;
import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;

/**
 * A basic implementation of {@link JobflowReference}.
 */
public class BasicJobflowReference extends BasicAttributeContainer implements JobflowReference {

    private final JobflowInfo info;

    private final TaskReferenceMap tasks;

    private final Set<JobflowReference> blockers;

    /**
     * Creates a new instance.
     * @param info the structural information of this jobflow
     * @param tasks the task map
     * @param blockers the blocker jobflows
     */
    public BasicJobflowReference(
            JobflowInfo info,
            TaskReferenceMap tasks,
            Collection<? extends JobflowReference> blockers) {
        this.info = info;
        this.tasks = tasks;
        this.blockers = Collections.unmodifiableSet(new LinkedHashSet<>(blockers));
    }

    @Override
    public String getFlowId() {
        return info.getFlowId();
    }

    @Override
    public ClassDescription getDescriptionClass() {
        return info.getDescriptionClass();
    }

    @Override
    public Collection<? extends TaskReference> getTasks(TaskReference.Phase phase) {
        return tasks.getTasks(phase);
    }

    @Override
    public Set<JobflowReference> getBlockers() {
        return blockers;
    }

    @Override
    public String toString() {
        return info.toString();
    }
}
