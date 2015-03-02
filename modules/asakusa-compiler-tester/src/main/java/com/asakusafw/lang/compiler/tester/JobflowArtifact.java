package com.asakusafw.lang.compiler.tester;

import com.asakusafw.lang.compiler.api.reference.ExternalPortReferenceMap;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * Represents a compilation result of Asakusa jobflow.
 */
public class JobflowArtifact {

    private final BatchInfo batch;

    private final JobflowReference reference;

    private final ExternalPortReferenceMap externalPorts;

    /**
     * Creates a new instance.
     * @param batch the container batch information
     * @param reference the compilation result
     * @param externalPorts the external port map
     */
    public JobflowArtifact(BatchInfo batch, JobflowReference reference, ExternalPortReferenceMap externalPorts) {
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
    public ExternalPortReferenceMap getExternalPorts() {
        return externalPorts;
    }
}
