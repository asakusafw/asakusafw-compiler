package com.asakusafw.lang.compiler.core.util;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.core.BatchCompiler.Context;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.model.graph.Batch;

/**
 * Puts {@link BatchReference compilation result} as extension.
 */
public class BatchReferenceCollector extends AbstractCompilerParticipant {

    /**
     * Returns the collected {@link BatchReference}.
     * @param context the target context
     * @return the {@link BatchReference}, or {@code null} if compilation has not been finished
     */
    public static BatchReference get(Context context) {
        return context.getExtension(BatchReference.class);
    }

    @Override
    public void afterBatch(Context context, Batch batch, BatchReference reference) {
        context.registerExtension(BatchReference.class, reference);
    }
}
