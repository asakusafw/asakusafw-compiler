package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;

import com.asakusafw.lang.compiler.model.info.JobflowInfo;

/**
 * A symbol of jobflow.
 */
public interface JobflowReference extends JobflowInfo, Reference, TaskReferenceMap {

    /**
     * Returns the tasks which must be executed in the specified phase on this jobflow.
     * @param phase the target phase
     * @return the element tasks, or an empty collection if there are no tasks in the specified phase
     */
    @Override
    Collection<? extends TaskReference> getTasks(TaskReference.Phase phase);

    /**
     * Returns jobflows which must be executed before this jobflow.
     * @return the blocker jobflows
     */
    Collection<? extends JobflowReference> getBlockerJobflows();
}
