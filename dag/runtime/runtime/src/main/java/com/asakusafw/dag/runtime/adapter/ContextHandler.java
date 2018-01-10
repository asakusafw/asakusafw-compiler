/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
import com.asakusafw.lang.utils.common.InterruptibleIo;

/**
 * Handles {@link ProcessorContext} and manages its lifecycle.
 * @param <T> the context type
 * @since 0.4.0
 */
public interface ContextHandler<T extends ProcessorContext> {

    /**
     * Starts a new session for the target context.
     * @param context the current processor context
     * @return the session handle
     * @throws IOException if I/O error was occurred while initializing session
     * @throws InterruptedException if interrupted while initializing session
     */
    Session start(T context) throws IOException, InterruptedException;

    /**
     * Represents a session for {@link ContextHandler}.
     */
    interface Session extends InterruptibleIo {
        // no special members
    }
}
