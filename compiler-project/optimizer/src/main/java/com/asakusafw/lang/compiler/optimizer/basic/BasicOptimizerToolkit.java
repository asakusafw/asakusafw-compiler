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
package com.asakusafw.lang.compiler.optimizer.basic;

import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.optimizer.OptimizerToolkit;

/**
 * A basic implementation of {@link OptimizerToolkit}.
 * @since 0.3.1
 */
public class BasicOptimizerToolkit implements OptimizerToolkit {

    @Override
    public void repair(OperatorGraph graph) {
        graph.rebuild();
    }

    @Override
    public boolean hasEffectiveOpposites(OperatorInput port) {
        return port.hasOpposites();
    }

    @Override
    public boolean hasEffectiveOpposites(OperatorOutput port) {
        return port.hasOpposites();
    }
}
