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
package com.asakusafw.dag.runtime.adapter;

import java.io.IOException;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.runtime.core.Result;

/**
 * An adapter for vertex operations.
 * Each implementation must have a constructor which only accepts {@link VertexProcessorContext}.
 * @param <T> the operation input type
 * @since 0.4.0
 */
public interface OperationAdapter<T> extends VertexElementAdapter {

    /**
     * Creates a new {@link Operation} instance.
     * @param context the current context
     * @return the target operation
     * @throws IOException if I/O error was occurred while building the operation
     * @throws InterruptedException if interrupted while building the operation
     */
    Operation<? super T> newInstance(Context context) throws IOException, InterruptedException;

    /**
     * Represents a context for {@link OperationAdapter}.
     * @since 0.4.0
     */
    public interface Context extends ProcessorContext {

        /**
         * Returns the data table for the specified ID.
         * @param <T> the object type
         * @param type the object type
         * @param id the table ID
         * @return {@link DataTable} object for the ID
         */
        <T> DataTable<T> getDataTable(Class<T> type, String id);

        /**
         * Returns the result sink for the specified ID.
         * @param <T> the object type
         * @param type the object type
         * @param id the output ID
         * @return {@link Result} object for output
         */
        <T> Result<T> getSink(Class<T> type, String id);
    }
}
