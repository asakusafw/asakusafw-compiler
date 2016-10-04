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

import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.VertexProcessorContext;

/**
 * An adapter for vertex outputs.
 * Each implementation must have a constructor which only accepts {@link VertexProcessorContext}.
 * @since 0.4.0
 */
public interface OutputAdapter extends VertexElementAdapter {

    /**
     * Creates a new {@link OutputHandler}.
     * @return the created handler
     * @throws IOException if I/O error was occurred while creating a new {@link OutputHandler}
     * @throws InterruptedException if interrupted while creating a new {@link OutputHandler}
     */
    OutputHandler<? super TaskProcessorContext> newHandler() throws IOException, InterruptedException;
}
