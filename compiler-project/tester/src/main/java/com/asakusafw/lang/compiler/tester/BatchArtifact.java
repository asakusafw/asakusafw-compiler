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
package com.asakusafw.lang.compiler.tester;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.BatchReference;

/**
 * Represents a compilation result of Asakusa batch.
 */
public class BatchArtifact {

    private final BatchReference reference;

    private final Map<String, JobflowArtifact> jobflows;

    /**
     * Creates a new instance.
     * @param reference the compilation result
     * @param jobflows the jobflow artifacts in this batch
     */
    public BatchArtifact(BatchReference reference, Collection<? extends JobflowArtifact> jobflows) {
        this.reference = reference;
        Map<String, JobflowArtifact> map = new LinkedHashMap<>();
        for (JobflowArtifact jobflow : jobflows) {
            map.put(jobflow.getReference().getFlowId(), jobflow);
        }
        this.jobflows = Collections.unmodifiableMap(map);
    }

    /**
     * Returns the compilation result.
     * @return the compilation result
     */
    public BatchReference getReference() {
        return reference;
    }

    /**
     * Returns a jobflow artifact in this batch.
     * @param flowId the target flow ID
     * @return the target jobflow artifact, or {@code null} if there is no such a jobflow
     */
    public JobflowArtifact findJobflow(String flowId) {
        return jobflows.get(flowId);
    }

    /**
     * Returns the jobflow artifacts in this batch.
     * @return the jobflow artifacts
     */
    public Set<JobflowArtifact> getJobflows() {
        return new LinkedHashSet<>(jobflows.values());
    }
}
