/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents a marker operator.
 * @since 0.1.0
 * @version 0.3.0
 */
public final class MarkerOperator extends Operator {

    /**
     * The port name.
     */
    public static final String PORT_NAME = "port"; //$NON-NLS-1$

    private MarkerOperator() {
        return;
    }

    @Override
    public MarkerOperator copy() {
        return copyAttributesTo(new MarkerOperator());
    }

    /**
     * Creates a new marker operator builder.
     * Marker operators always have a single input, a single output, and no arguments.
     * Then clients can only edit their {@link #getAttribute(Class) attributes}.
     * @param dataType the data type
     * @return the builder
     */
    public static Builder builder(TypeDescription dataType) {
        return new Builder(new InternalBuilder(new MarkerOperator())
                .input(PORT_NAME, dataType)
                .output(PORT_NAME, dataType)
                .build());
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.MARKER;
    }

    /**
     * Returns the a single operator input of this.
     * @return the operator input
     */
    public OperatorInput getInput() {
        assert getInputs().size() == 1;
        return getInput(0);
    }

    /**
     * Returns the a single operator output of this.
     * @return the operator output
     */
    public OperatorOutput getOutput() {
        assert getOutputs().size() == 1;
        return getOutput(0);
    }

    /**
     * Returns the data type on this operator.
     * @return the data type
     */
    public TypeDescription getDataType() {
        return getInput().getDataType();
    }

    @Override
    public String toString() {
        if (attributes.isEmpty()) {
            return "MarkerOperator()"; //$NON-NLS-1$
        } else {
            Set<Class<?>> types = getAttributeTypes();
            Class<?> a = types.iterator().next();
            if (attributes.size() == 1) {
                return MessageFormat.format(
                        "MarkerOperator({0}={1})", //$NON-NLS-1$
                        a.getSimpleName(),
                        attributes.get(a));
            } else {
                return MessageFormat.format(
                        "MarkerOperator({0}={1}, ...)", //$NON-NLS-1$
                        a.getSimpleName(),
                        attributes.get(a));
            }
        }
    }

    private static final class InternalBuilder extends AbstractBuilder<MarkerOperator, InternalBuilder> {

        InternalBuilder(MarkerOperator owner) {
            super(owner);
        }

        @Override
        protected InternalBuilder getSelf() {
            return this;
        }
    }

    /**
     * A builder for {@link MarkerOperator}.
     * @since 0.1.0
     * @version 0.3.0
     */
    public static final class Builder extends BuilderBase<MarkerOperator, Builder> {

        Builder(MarkerOperator owner) {
            super(owner);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }
    }
}
