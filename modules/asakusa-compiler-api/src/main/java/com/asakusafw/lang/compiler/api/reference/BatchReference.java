package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;

import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * A symbol of batch.
 */
public interface BatchReference extends BatchInfo, JobflowReferenceMap {

    /**
     * Returns the jobflows which must be executed in this batch.
     * @return the element jobflows (may not empty)
     */
    @Override
    Collection<? extends JobflowReference> getJobflows();
}
