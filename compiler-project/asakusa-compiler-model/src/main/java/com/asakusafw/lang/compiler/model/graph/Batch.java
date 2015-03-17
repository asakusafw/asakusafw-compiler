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
package com.asakusafw.lang.compiler.model.graph;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * Represents a batch.
 */
public class Batch extends BatchInfo.Basic {

    private final Map<String, BatchElement> elements = new LinkedHashMap<>();

    /**
     * Creates a new instance.
     * @param batchId the batch ID
     * @param descriptionClass the original batch description class
     * @param comment a comment for this batch (nullable)
     * @param parameters parameters for this batch
     * @param attributes extra attributes for this batch
     */
    public Batch(
            String batchId,
            ClassDescription descriptionClass,
            String comment,
            Collection<Parameter> parameters,
            Collection<Attribute> attributes) {
        super(batchId, descriptionClass, comment, parameters, attributes);
    }

    /**
     * Creates a new instance.
     * @param info the structural information of this batch
     */
    public Batch(BatchInfo info) {
        this(info.getBatchId(), info.getDescriptionClass(),
                info.getComment(), info.getParameters(), info.getAttributes());
    }

    /**
     * Adds a jobflow to this batch.
     * @param jobflow the jobflow
     * @return the related batch element
     */
    public BatchElement addElement(Jobflow jobflow) {
        String flowId = jobflow.getFlowId();
        if (elements.containsKey(flowId)) {
            throw new IllegalStateException();
        }
        BatchElement element = new BatchElement(this, jobflow);
        elements.put(flowId, element);
        return element;
    }

    /**
     * Returns a batch element about the specified flow ID.
     * @param flowId the target flow ID
     * @return the corresponded batch element, or {@code null} if there are no such an element
     */
    public BatchElement findElement(String flowId) {
        return elements.get(flowId);
    }

    /**
     * Returns a batch element about the specified jobflow.
     * @param jobflow the target jobflow
     * @return the corresponded batch element, or {@code null} if there are no such an element
     */
    public BatchElement findElement(Jobflow jobflow) {
        return findElement(jobflow.getFlowId());
    }

    /**
     * Returns all elements in this batch.
     * @return all elements
     */
    public Set<BatchElement> getElements() {
        return new LinkedHashSet<>(elements.values());
    }
}
