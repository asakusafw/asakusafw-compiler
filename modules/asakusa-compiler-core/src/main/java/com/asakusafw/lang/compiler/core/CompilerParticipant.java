package com.asakusafw.lang.compiler.core;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * A participant for Asakusa DSL compilers.
 */
public interface CompilerParticipant {

    /**
     * Run before compiling batch.
     * @param context the current context
     * @param batch the target batch
     */
    void beforeBatch(BatchCompiler.Context context, Batch batch);

    /**
     * Run after compiling batch.
     * @param context the current context
     * @param batch the target batch
     * @param reference the compilation result
     */
    void afterBatch(BatchCompiler.Context context, Batch batch, BatchReference reference);

    /**
     * Run before compiling jobflow.
     * @param context the current context
     * @param batch information of the jobflow owner
     * @param jobflow the target jobflow
     */
    void beforeJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow);

    /**
     * Run after compiling jobflow.
     * @param context the current context
     * @param batch information of the jobflow owner
     * @param jobflow the target jobflow
     */
    void afterJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow);
}
