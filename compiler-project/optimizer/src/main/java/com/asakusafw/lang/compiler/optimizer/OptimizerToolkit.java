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
package com.asakusafw.lang.compiler.optimizer;

import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;

/**
 * Toolkit for optimizers.
 * @since 0.3.1
 */
public interface OptimizerToolkit {

    /**
     * Repairs the target operator graph if its invariants are broken.
     * @param graph the target operator graph
     */
    void repair(OperatorGraph graph);

    /**
     * Returns whether or not the target input has one or more effective upstream ports.
     * @param port the target input
     * @return {@code true} if the target input has one or more effective upstream ports, otherwise {@code false}
     */
    boolean hasEffectiveOpposites(OperatorInput port);

    /**
     * Returns whether or not the target output has one or more effective downstream ports.
     * @param port the target input
     * @return {@code true} if the target output has one or more effective downstream ports, otherwise {@code false}
     */
    boolean hasEffectiveOpposites(OperatorOutput port);
}
