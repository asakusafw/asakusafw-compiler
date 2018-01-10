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
package com.asakusafw.lang.compiler.tester;

import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * Represents a compilation result of Asakusa jobflow.
 */
public class JobflowArtifact {

    private final BatchInfo batch;

    private final JobflowReference reference;

    private final ExternalPortMap externalPorts;

    /**
     * Creates a new instance.
     * @param batch the container batch information
     * @param reference the compilation result
     * @param externalPorts the external port map
     */
    public JobflowArtifact(BatchInfo batch, JobflowReference reference, ExternalPortMap externalPorts) {
        this.batch = batch;
        this.reference = reference;
        this.externalPorts = externalPorts;
    }

    /**
     * Returns the container batch information.
     * @return the container batch information
     */
    public BatchInfo getBatch() {
        return batch;
    }

    /**
     * Returns the compilation result.
     * @return the compilation result
     */
    public JobflowReference getReference() {
        return reference;
    }

    /**
     * Returns the external port map for this jobflow.
     * @return the external port map
     */
    public ExternalPortMap getExternalPorts() {
        return externalPorts;
    }
}
