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
package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;

/**
 * An abstract super interface of providing {@link TaskReference}s.
 */
public interface TaskReferenceMap {

    /**
     * Returns the tasks about the specified phase.
     * @param phase the target phase
     * @return the element tasks, or an empty collection if there are no tasks in the specified phase
     */
    Collection<? extends TaskReference> getTasks(TaskReference.Phase phase);
}
