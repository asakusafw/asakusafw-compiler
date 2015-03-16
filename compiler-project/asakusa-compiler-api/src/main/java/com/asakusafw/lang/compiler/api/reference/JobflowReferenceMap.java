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
