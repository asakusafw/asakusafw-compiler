package com.asakusafw.lang.compiler.api.basic;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;

/**
 * A basic implementation of {@link JobflowReference}.
 */
public class BasicJobflowReference implements JobflowReference {

    private final JobflowInfo info;

    private final TaskReferenceMap tasks;

    private final Set<JobflowReference> blockers;

    /**
     * Creates a new instance.
     * @param info the structural information of this jobflow
     * @param tasks the task map
     * @param blockers the blocker jobflows
     */
    public BasicJobflowReference(
            JobflowInfo info,
            TaskReferenceMap tasks,
            Collection<? extends JobflowReference> blockers) {
        this.info = info;
        this.tasks = tasks;
        this.blockers = Collections.unmodifiableSet(new LinkedHashSet<>(blockers));
    }

    @Override
    public String getFlowId() {
        return info.getFlowId();
    }

    @Override
    public ClassDescription getDescriptionClass() {
        return info.getDescriptionClass();
    }

    @Override
    public Collection<? extends TaskReference> getTasks(TaskReference.Phase phase) {
        return tasks.getTasks(phase);
    }

    @Override
    public Set<JobflowReference> getBlockerJobflows() {
        return blockers;
    }

    @Override
    public String toString() {
        return info.toString();
    }
}
