/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Represents a nested flow operator.
 */
public final class FlowOperator extends Operator implements Flow {

    private final ClassDescription descriptionClass;

    private final OperatorGraph operatorGraph;

    private FlowOperator(ClassDescription descriptionClass, OperatorGraph operatorGraph) {
        this.descriptionClass = descriptionClass;
        this.operatorGraph = operatorGraph;
    }

    @Override
    public FlowOperator copy() {
        return copyAttributesTo(new FlowOperator(descriptionClass, operatorGraph.copy()));
    }

    /**
     * Creates a new builder.
     * @param descriptionClass the flow description class name
     * @param operatorGraph the operator graph
     * @return the builder
     */
    public static Builder builder(ClassDescription descriptionClass, OperatorGraph operatorGraph) {
        return new Builder(new FlowOperator(descriptionClass, operatorGraph));
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.FLOW;
    }

    @Override
    public ClassDescription getDescriptionClass() {
        return descriptionClass;
    }

    @Override
    public OperatorGraph getOperatorGraph() {
        return operatorGraph;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "FlowOperator({0})", //$NON-NLS-1$
                getDescriptionClass().getSimpleName());
    }

    /**
     * A builder for {@link FlowOperator}.
     */
    public static final class Builder extends AbstractBuilder<FlowOperator, Builder> {

        Builder(FlowOperator owner) {
            super(owner);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }
    }
}
