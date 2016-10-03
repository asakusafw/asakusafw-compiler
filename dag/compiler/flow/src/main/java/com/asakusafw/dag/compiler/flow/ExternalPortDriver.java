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
package com.asakusafw.dag.compiler.flow;

import com.asakusafw.dag.compiler.model.build.GraphInfoBuilder;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;

/**
 * Processes external I/O ports.
 * @since 0.4.0
 */
public interface ExternalPortDriver {

    /**
     * Returns whether or not this can accept the target input.
     * @param port the target port
     * @return {@code true} if this can accept the port, otherwise {@code false}
     */
    boolean accepts(ExternalInput port);

    /**
     * Returns whether or not this can accept the target output.
     * @param port the target port
     * @return {@code true} if this can accept the port, otherwise {@code false}
     */
    boolean accepts(ExternalOutput port);

    /**
     * Processes the input and returns an implementation of {@link InputAdapter}.
     * @param port the target port
     * @return the related class
     */
    ClassDescription processInput(ExternalInput port);

    /**
     * Processes the outputs in the source plan, and registers the corresponded vertices into the building graph.
     * @param target the target graph builder
     */
    void processOutputs(GraphInfoBuilder target);

    /**
     * Processes the outputs in the source plan, and registers the corresponded vertices into the building graph.
     * @param target the target graph builder
     */
    default void processPlan(GraphInfoBuilder target) {
        return;
    }
}
