/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
    default void beforeBatch(BatchCompiler.Context context, Batch batch) {
        return;
    }

    /**
     * Run after compiling batch.
     * @param context the current context
     * @param batch the target batch
     * @param reference the compilation result
     */
    default void afterBatch(BatchCompiler.Context context, Batch batch, BatchReference reference) {
        return;
    }

    /**
     * Run before compiling jobflow.
     * @param context the current context
     * @param batch information of the jobflow owner
     * @param jobflow the target jobflow
     */
    default void beforeJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow) {
        return;
    }

    /**
     * Run after compiling jobflow.
     * @param context the current context
     * @param batch information of the jobflow owner
     * @param jobflow the target jobflow
     */
    default void afterJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow) {
        return;
    }
}
