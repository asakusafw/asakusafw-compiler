/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReferenceMap;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * A basic implementation of {@link BatchReference}.
 */
public class BasicBatchReference implements BatchReference {

    private final BatchInfo info;

    private final JobflowReferenceMap jobflows;

    /**
     * Creates a new instance.
     * @param info the structural information of this batch
     * @param jobflows the element jobflows
     */
    public BasicBatchReference(BatchInfo info, Collection<? extends JobflowReference> jobflows) {
        this.info = info;
        this.jobflows = new JobflowContainer(jobflows);
    }

    /**
     * Creates a new instance.
     * @param info the structural information of this batch
     * @param jobflows the element jobflows
     */
    public BasicBatchReference(BatchInfo info, JobflowReferenceMap jobflows) {
        this.info = info;
        this.jobflows = new JobflowContainer(jobflows.getJobflows());
    }

    @Override
    public String getBatchId() {
        return info.getBatchId();
    }

    @Override
    public ClassDescription getDescriptionClass() {
        return info.getDescriptionClass();
    }

    @Override
    public String getComment() {
        return info.getComment();
    }

    @Override
    public Collection<Parameter> getParameters() {
        return info.getParameters();
    }

    @Override
    public Set<Attribute> getAttributes() {
        return info.getAttributes();
    }

    @Override
    public JobflowReference find(String flowId) {
        return jobflows.find(flowId);
    }

    @Override
    public Set<JobflowReference> getJobflows() {
        return new LinkedHashSet<>(jobflows.getJobflows());
    }
}
