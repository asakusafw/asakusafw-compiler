/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.vanilla.core.engine;

import java.io.IOException;

import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.vanilla.core.mirror.GraphMirror;
import com.asakusafw.vanilla.core.mirror.VertexMirror;

/**
 * A vertex scheduler.
 * @since 0.4.0
 */
@FunctionalInterface
public interface VertexScheduler {

    /**
     * Returns an execution schedule of the given graph.
     * @param graph the target graph
     * @return a sequence of vertices sorted with their execution order
     */
    Stream schedule(GraphMirror graph);

    /**
     * A vertex stream.
     * @since 0.4.0
     */
    @FunctionalInterface
    interface Stream extends InterruptibleIo {

        /**
         * Returns the next vertex.
         * @return the next vertex, or {@code null} if there are no more vertices
         * @throws IOException if I/O error was occurred while computing the next element
         * @throws InterruptedException if interrupted while computing the next element
         */
        VertexMirror poll() throws IOException, InterruptedException;

        @Override
        default void close() throws IOException, InterruptedException {
            return;
        }
    }
}
