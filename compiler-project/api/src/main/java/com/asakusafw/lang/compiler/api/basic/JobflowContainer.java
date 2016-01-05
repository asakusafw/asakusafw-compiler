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
package com.asakusafw.lang.compiler.api.basic;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReferenceMap;

/**
 * Holds {@link JobflowReference} objects for each runtime phase.
 */
public class JobflowContainer implements JobflowReferenceMap {

    private final Map<String, JobflowReference> elements = new LinkedHashMap<>();

    private final Set<JobflowReference> saw = new HashSet<>();

    /**
     * Creates a new empty instance.
     */
    public JobflowContainer() {
        return;
    }

    /**
     * Creates a new instance.
     * @param jobflows the elements
     */
    public JobflowContainer(Collection<? extends JobflowReference> jobflows) {
        for (JobflowReference jobflow : jobflows) {
            if (saw.contains(jobflow)) {
                continue;
            }
            String id = jobflow.getFlowId();
            if (elements.containsKey(id)) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "duplicate flow ID: {0} ({1} <-> {2})",
                        id,
                        jobflow.getDescriptionClass(),
                        elements.get(id).getDescriptionClass()));
            }
            saw.add(jobflow);
            elements.put(id, jobflow);
        }
        for (JobflowReference jobflow : jobflows) {
            for (JobflowReference blocker : jobflow.getBlockers()) {
                if (saw.contains(blocker) == false) {
                    throw new IllegalArgumentException(MessageFormat.format(
                            "blocker jobflow is not found: {0}->{1}",
                            jobflow,
                            blocker));
                }
            }
        }
    }

    /**
     * Adds a jobflow to this container.
     * Each {@link JobflowReference#getBlockers() blocker jobflow} must have been added.
     * If the task is already added to this, this will do nothing.
     * @param jobflow the target jobflow
     */
    public void add(JobflowReference jobflow) {
        if (saw.contains(jobflow)) {
            return;
        }
        String flowId = jobflow.getFlowId();
        if (elements.containsKey(flowId)) {
            throw new IllegalArgumentException();
        }
        for (JobflowReference blocker : jobflow.getBlockers()) {
            if (saw.contains(blocker) == false) {
                throw new IllegalStateException(MessageFormat.format(
                        "blocker jobflow is not found: {0}->{1}",
                        jobflow,
                        blocker));
            }
        }
        saw.add(jobflow);
        elements.put(flowId, jobflow);
    }

    @Override
    public JobflowReference find(String flowId) {
        return elements.get(flowId);
    }

    @Override
    public List<JobflowReference> getJobflows() {
        return new ArrayList<>(elements.values());
    }
}
