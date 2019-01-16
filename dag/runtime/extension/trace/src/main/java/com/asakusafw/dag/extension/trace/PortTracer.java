/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.dag.extension.trace;

import java.util.function.Consumer;

/**
 * An abstract super interface which provides object sinks for tracing.
 * @since 0.4.0
 */
public interface PortTracer {

    /**
     * Returns whether the target vertex MAY support tracing facilities or not.
     * @param vertexId the target vertex ID
     * @return {@code true} if the target vertex may support tracing facilities,
     *      or {@code false} if the vertex never support it
     */
    default boolean isSupported(String vertexId) {
        return true;
    }

    /**
     * Returns a trace sink for the target port.
     * @param vertexId the target vertex ID
     * @param portId the target port ID
     * @return the related trace sink, or {@code null} if it id not supported
     */
    Consumer<Object> getSink(String vertexId, String portId);
}
