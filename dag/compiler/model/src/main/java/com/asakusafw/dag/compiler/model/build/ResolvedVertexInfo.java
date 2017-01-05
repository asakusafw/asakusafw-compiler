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
package com.asakusafw.dag.compiler.model.build;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.asakusafw.dag.api.model.VertexDescriptor;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents a revolved vertex.
 * @since 0.4.0
 */
public class ResolvedVertexInfo {

    private final String id;

    private final VertexDescriptor descriptor;

    private final Map<SubPlan.Input, ResolvedInputInfo> inputs;

    private final Map<SubPlan.Output, ResolvedOutputInfo> outputs;

    private final Set<ResolvedVertexInfo> implicitDependencies;

    /**
     * Creates a new instance.
     * @param id the vertex ID
     * @param descriptor the vertex descriptor
     * @param inputs the inputs
     * @param outputs the outputs
     */
    public ResolvedVertexInfo(
            String id,
            VertexDescriptor descriptor,
            Map<? extends SubPlan.Input, ? extends ResolvedInputInfo> inputs,
            Map<? extends SubPlan.Output, ? extends ResolvedOutputInfo> outputs) {
        this(id, descriptor, inputs, outputs, Collections.emptySet());
    }

    /**
     * Creates a new instance.
     * @param id the vertex ID
     * @param descriptor the vertex descriptor
     * @param inputs the inputs
     * @param outputs the outputs
     * @param implicitDependencies the implicit dependency targets
     */
    public ResolvedVertexInfo(
            String id,
            VertexDescriptor descriptor,
            Map<? extends SubPlan.Input, ? extends ResolvedInputInfo> inputs,
            Map<? extends SubPlan.Output, ? extends ResolvedOutputInfo> outputs,
            Collection<? extends ResolvedVertexInfo> implicitDependencies) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(descriptor);
        Arguments.requireNonNull(inputs);
        Arguments.requireNonNull(outputs);
        Arguments.requireNonNull(implicitDependencies);
        this.id = id;
        this.descriptor = descriptor;
        this.inputs = Arguments.freeze(inputs);
        this.outputs = Arguments.freeze(outputs);
        this.implicitDependencies = Arguments.copyToSet(implicitDependencies);
    }

    /**
     * Returns the ID.
     * @return the ID
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the descriptor.
     * @return the descriptor
     */
    public VertexDescriptor getDescriptor() {
        return descriptor;
    }

    /**
     * Returns the inputs.
     * @return the inputs
     */
    public Map<SubPlan.Input, ResolvedInputInfo> getInputs() {
        return inputs;
    }

    /**
     * Returns the outputs.
     * @return the outputs
     */
    public Map<SubPlan.Output, ResolvedOutputInfo> getOutputs() {
        return outputs;
    }

    /**
     * Adds an implicit dependency.
     * @param dependency the target vertex
     */
    public void addImplicitDependency(ResolvedVertexInfo dependency) {
        this.implicitDependencies.add(dependency);
    }

    /**
     * Returns the implicit dependency targets.
     * @return the implicit dependency targets
     */
    public Set<ResolvedVertexInfo> getImplicitDependencies() {
        return Collections.unmodifiableSet(implicitDependencies);
    }
}
