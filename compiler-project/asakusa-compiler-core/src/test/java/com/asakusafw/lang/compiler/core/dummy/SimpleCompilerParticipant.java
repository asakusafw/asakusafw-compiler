package com.asakusafw.lang.compiler.core.dummy;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.CompilerParticipant;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * Mock implementation of {@link SimpleCompilerParticipant}.
 */
public class SimpleCompilerParticipant implements CompilerParticipant {

    /**
     * Returns whether the processor was activated in the context.
     * @param context the current context
     * @return {@code true} if this processor was activated
     */
    public static boolean contains(JobflowCompiler.Context context) {
        return dig(context).isActiavated();
    }

    /**
     * Returns whether the processor was activated in the context.
     * @param context the current context
     * @return {@code true} if this processor was activated
     */
    public static boolean contains(BatchCompiler.Context context) {
        return dig(context).isActiavated();
    }

    @Override
    public void beforeBatch(BatchCompiler.Context context, Batch batch) {
        dig(context).before();
    }

    @Override
    public void afterBatch(BatchCompiler.Context context, Batch batch, BatchReference reference) {
        dig(context).after();
        context.registerExtension(BatchReference.class, reference);
    }

    @Override
    public void beforeJobflow(JobflowCompiler.Context context, BatchInfo batch,
            Jobflow jobflow) {
        dig(context).before();
    }

    @Override
    public void afterJobflow(JobflowCompiler.Context context, BatchInfo batch,
            Jobflow jobflow) {
        dig(context).after();
    }

    private static Status dig(ExtensionContainer.Editable container) {
        Status status = container.getExtension(Status.class);
        if (status == null) {
            status = new Status();
            container.registerExtension(Status.class, status);
        }
        return status;
    }

    private static final class Status {

        private boolean before = false;

        private boolean after = false;

        Status() {
            return;
        }

        void before() {
            if (before) {
                throw new IllegalStateException();
            }
            before = true;
        }

        void after() {
            if (after) {
                throw new IllegalStateException();
            }
            after = true;
        }

        boolean isActiavated() {
            return before && after;
        }
    }
}
