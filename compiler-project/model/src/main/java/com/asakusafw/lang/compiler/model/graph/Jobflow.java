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
package com.asakusafw.lang.compiler.model.graph;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;

/**
 * Represents a jobflow.
 */
public class Jobflow extends JobflowInfo.Basic implements Flow {

    private final OperatorGraph operatorGraph;

    /**
     * Creates a new instance.
     * @param flowId the flow ID
     * @param descriptionClass the original flow description class name
     * @param operatorGraph the operator graph
     */
    public Jobflow(String flowId, ClassDescription descriptionClass, OperatorGraph operatorGraph) {
        super(flowId, descriptionClass);
        this.operatorGraph = operatorGraph;
    }

    /**
     * Creates a new instance.
     * @param info the structural information of this jobflow
     * @param operatorGraph the operator graph
     */
    public Jobflow(JobflowInfo info, OperatorGraph operatorGraph) {
        this(info.getFlowId(), info.getDescriptionClass(), operatorGraph);
    }

    @Override
    public OperatorGraph getOperatorGraph() {
        return operatorGraph;
    }
}
