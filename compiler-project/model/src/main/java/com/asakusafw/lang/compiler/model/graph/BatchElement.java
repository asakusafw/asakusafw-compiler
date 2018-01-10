/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

/**
 * An element in {@link Batch}.
 */
public class BatchElement {

    private final Batch owner;

    private final Jobflow jobflow;

    private final Set<BatchElement> predecessors = new HashSet<>();

    private final Set<BatchElement> successors = new HashSet<>();

    /**
     * Creates a new instance.
     * @param owner the owner
     * @param jobflow the jobflow
     */
    BatchElement(Batch owner, Jobflow jobflow) {
        this.owner = owner;
        this.jobflow = jobflow;
    }

    /**
     * Returns the owner of this element.
     * @return the owner
     */
    public Batch getOwner() {
        return owner;
    }

    /**
     * Returns the jobflow for this element.
     * @return the jobflow
     */
    public Jobflow getJobflow() {
        return jobflow;
    }

    /**
     * Returns blocker elements for this.
     * @return the blocker elements
     */
    public Set<BatchElement> getBlockerElements() {
        return new HashSet<>(predecessors);
    }

    /**
     * Returns elements which are blocked by this.
     * @return blocking elements
     */
    public Set<BatchElement> getBlockingElements() {
        return new HashSet<>(successors);
    }

    /**
     * Adds a blocker element for this.
     * @param blocker the blocker element
     */
    public void addBlockerElement(BatchElement blocker) {
        if (owner != blocker.owner) {
            throw new IllegalArgumentException();
        }
        this.predecessors.add(blocker);
        blocker.successors.add(this);
    }

    /**
     * Removes a blocker element from this.
     * @param blocker the blocker element
     */
    public void removeBlockerElement(BatchElement blocker) {
        this.predecessors.remove(blocker);
        blocker.successors.remove(this);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "BatchElement({0})", //$NON-NLS-1$
                jobflow.getFlowId());
    }
}
