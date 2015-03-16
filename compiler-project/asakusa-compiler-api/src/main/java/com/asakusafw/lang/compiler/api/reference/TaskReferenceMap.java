package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;

/**
 * An abstract super interface of providing {@link TaskReference}s.
 */
public interface TaskReferenceMap {

    /**
     * Returns the tasks about the specified phase.
     * @param phase the target phase
     * @return the element tasks, or an empty collection if there are no tasks in the specified phase
     */
    Collection<? extends TaskReference> getTasks(TaskReference.Phase phase);
}
