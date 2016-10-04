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
package com.asakusafw.dag.compiler.extension.internalio;

import static com.asakusafw.dag.compiler.flow.DataFlowUtil.*;

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.flow.DagDescriptorFactory;
import com.asakusafw.dag.compiler.flow.ExternalPortDriver;
import com.asakusafw.dag.compiler.flow.ExternalPortDriverProvider;
import com.asakusafw.dag.compiler.model.build.GraphInfoBuilder;
import com.asakusafw.dag.compiler.model.plan.VertexSpec;
import com.asakusafw.lang.compiler.internalio.InternalIoConstants;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * An implementation of {@link ExternalPortDriver} for internal I/O ports.
 * @since 0.4.0
 */
public class InternalPortDriver implements ExternalPortDriver {

    static final Logger LOG = LoggerFactory.getLogger(InternalPortDriver.class);

    private final ClassGeneratorContext context;

    private final DagDescriptorFactory descriptors;

    private final Map<ExternalInput, String> inputModels;

    private final Map<ExternalOutput, String> outputModels;

    private final Map<ExternalInput, VertexSpec> inputOwners;

    private final Map<ExternalOutput, VertexSpec> outputOwners;

    InternalPortDriver(ExternalPortDriverProvider.Context context) {
        Arguments.requireNonNull(context);
        this.context = context.getGeneratorContext();
        this.descriptors = context.getDescriptorFactory();

        Plan plan = context.getSourcePlan();
        this.inputModels = collectModels(
                collectOperators(plan, ExternalInput.class),
                InternalIoConstants.MODULE_NAME, String.class);
        this.outputModels = collectModels(
                collectOperators(plan, ExternalOutput.class),
                InternalIoConstants.MODULE_NAME, String.class);
        this.inputOwners = collectOwners(plan, inputModels.keySet());
        this.outputOwners = collectOwners(plan, outputModels.keySet());
    }

    @Override
    public boolean accepts(ExternalInput port) {
        return inputModels.containsKey(port);
    }

    @Override
    public boolean accepts(ExternalOutput port) {
        return outputModels.containsKey(port);
    }

    @Override
    public ClassDescription processInput(ExternalInput port) {
        VertexSpec vertex = Invariants.requireNonNull(inputOwners.get(port));
        String path = Invariants.requireNonNull(inputModels.get(port));
        return generateInternalInput(context, vertex, port, Collections.singleton(path));
    }

    @Override
    public void processOutputs(GraphInfoBuilder target) {
        for (Map.Entry<ExternalOutput, VertexSpec> entry : outputOwners.entrySet()) {
            ExternalOutput port = entry.getKey();
            VertexSpec vertex = entry.getValue();
            if (isEmptyOutput(vertex)) {
                continue;
            }
            String path = Invariants.requireNonNull(outputModels.get(port));
            registerInternalOutput(context, descriptors, target, vertex, port, path);
        }
    }
}
