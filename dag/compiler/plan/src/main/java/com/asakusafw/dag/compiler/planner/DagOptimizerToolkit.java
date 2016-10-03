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
package com.asakusafw.dag.compiler.planner;

import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.optimizer.OptimizerToolkit;
import com.asakusafw.lang.compiler.optimizer.basic.BasicOptimizerToolkit;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.compiler.planning.Planning;

/**
 * An {@link OptimizerToolkit} for Asakusa DAG compiler.
 * @since 0.4.0
 */
public final class DagOptimizerToolkit extends BasicOptimizerToolkit {

    /**
     * The singleton instance.
     */
    public static final DagOptimizerToolkit INSTANCE = new DagOptimizerToolkit();

    private DagOptimizerToolkit() {
        return;
    }

    @Override
    public void repair(OperatorGraph graph) {
        Planning.normalize(graph);
    }

    @Override
    public boolean hasEffectiveOpposites(OperatorInput port) {
        return port.getOpposites().stream()
                .filter(o -> PlanMarkers.get(o.getOwner()) != PlanMarker.BEGIN)
                .findAny()
                .isPresent();
    }

    @Override
    public boolean hasEffectiveOpposites(OperatorOutput port) {
        return port.getOpposites().stream()
                .filter(o -> PlanMarkers.get(o.getOwner()) != PlanMarker.END)
                .findAny()
                .isPresent();
    }
}
