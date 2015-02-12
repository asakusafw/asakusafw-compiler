package com.asakusafw.lang.compiler.api.basic;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * A basic implementation of {@link BatchReference}.
 */
public class BasicBatchReference implements BatchReference {

    private final BatchInfo info;

    private final Set<JobflowReference> jobflows;

    /**
     * Creates a new instance.
     * @param info the structural information of this batch
     * @param jobflows the element jobflows
     */
    public BasicBatchReference(BatchInfo info, Collection<? extends JobflowReference> jobflows) {
        this.info = info;
        this.jobflows = Collections.unmodifiableSet(new LinkedHashSet<>(jobflows));
    }

    @Override
    public String getBatchId() {
        return info.getBatchId();
    }

    @Override
    public ClassDescription getDescriptionClass() {
        return info.getDescriptionClass();
    }

    @Override
    public String getComment() {
        return info.getComment();
    }

    @Override
    public Collection<Parameter> getParameters() {
        return info.getParameters();
    }

    @Override
    public Set<Attribute> getAttributes() {
        return info.getAttributes();
    }

    @Override
    public Set<JobflowReference> getJobflows() {
        return jobflows;
    }
}
