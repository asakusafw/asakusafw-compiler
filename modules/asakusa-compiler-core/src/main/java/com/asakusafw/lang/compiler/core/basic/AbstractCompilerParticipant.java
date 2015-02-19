package com.asakusafw.lang.compiler.core.basic;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.CompilerParticipant;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * An abstract implementation of {@link CompilerParticipant} with no actions.
 */
public abstract class AbstractCompilerParticipant implements CompilerParticipant {

    @Override
    public void beforeBatch(BatchCompiler.Context context, Batch batch) {
        return;
    }

    @Override
    public void afterBatch(BatchCompiler.Context context, Batch batch, BatchReference reference) {
        return;
    }

    @Override
    public void beforeJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow) {
        return;
    }

    @Override
    public void afterJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow) {
        return;
    }
}
