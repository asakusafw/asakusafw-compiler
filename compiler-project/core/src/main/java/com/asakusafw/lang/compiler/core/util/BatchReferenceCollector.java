/**
 * Copyright 2011-2016 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
