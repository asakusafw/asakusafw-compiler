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
package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;

/**
 * An abstract super interface of providing {@link JobflowReference}s.
 */
public interface JobflowReferenceMap {

    /**
     * Returns a jobflow in this map.
     * @param flowId the target flow ID
     * @return the related jobflow, or {@code null} if it is not defined
     */
    JobflowReference find(String flowId);

    /**
     * Returns the jobflows.
     * @return the element jobflows
     */
    Collection<? extends JobflowReference> getJobflows();
}
