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
package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;

import com.asakusafw.lang.compiler.model.info.JobflowInfo;

/**
 * A symbol of jobflow.
 */
public interface JobflowReference extends JobflowInfo, BlockingReference<JobflowReference>, TaskReferenceMap {

    /**
     * Returns the tasks which must be executed in the specified phase on this jobflow.
     * @param phase the target phase
     * @return the element tasks, or an empty collection if there are no tasks in the specified phase
     */
    @Override
    Collection<? extends TaskReference> getTasks(TaskReference.Phase phase);

    /**
     * Returns jobflows which must be executed before this jobflow.
     * @return the blocker jobflows
     */
    @Override
    Collection<? extends JobflowReference> getBlockers();
}
