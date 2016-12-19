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
package com.asakusafw.dag.compiler.flow.adapter;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.compiler.model.plan.InputSpec;
import com.asakusafw.dag.compiler.model.plan.InputSpec.InputType;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * An adapter implementation of context of {@link OperatorNodeGenerator}.
 * @since 0.4.0
 */
public class OperatorNodeGeneratorContextAdapter
        implements OperatorNodeGenerator.Context, ClassGeneratorContext.Forward {

    private final ClassGeneratorContext forward;

    private final SubPlan owner;

    private final Map<OperatorProperty, VertexElement> dependencies;

    /**
     * Creates a new instance.
     * @param forward the forwarding target
     * @param owner the owner
     * @param dependencies the dependencies
     */
    public OperatorNodeGeneratorContextAdapter(
            ClassGeneratorContext forward,
            SubPlan owner, Map<OperatorProperty, VertexElement> dependencies) {
        this.forward = forward;
        this.owner = owner;
        this.dependencies = dependencies;
    }

    @Override
    public ClassGeneratorContext getForward() {
        return forward;
    }

    @Override
    public VertexElement getDependency(OperatorProperty property) {
        return Invariants.requireNonNull(dependencies.get(property));
    }

    @Override
    public int getGroupIndex(OperatorInput input) {
        int index = 0;
        for (OperatorInput port : input.getOwner().getInputs()) {
            if (isGroupInput(port)) {
                if (port.equals(input)) {
                    return index;
                }
                index++;
            }
        }
        return -1;
    }

    private boolean isGroupInput(OperatorInput port) {
        if (port.getInputUnit() != InputUnit.GROUP) {
            return false;
        }
        Collection<OperatorOutput> opposites = port.getOpposites();
        if (opposites.size() != 1) {
            return false;
        }
        InputType type = opposites.stream()
            .map(OperatorPort::getOwner)
            .map(owner::findInput)
            .filter(Objects::nonNull)
            .map(p -> Invariants.requireNonNull(p.getAttribute(InputSpec.class)).getInputType())
            .findAny()
            .orElse(null);
        return type == InputType.CO_GROUP;
    }
}
