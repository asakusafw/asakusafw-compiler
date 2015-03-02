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
