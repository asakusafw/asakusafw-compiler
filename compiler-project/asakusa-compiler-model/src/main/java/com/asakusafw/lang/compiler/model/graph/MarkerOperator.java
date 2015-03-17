/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents a marker operator.
 * @see OperatorAttribute
 */
public final class MarkerOperator extends Operator {

    /**
     * The port name.
     */
    public static final String PORT_NAME = "port"; //$NON-NLS-1$

    final Map<Class<?>, Object> attributes = new HashMap<>();

    private MarkerOperator() {
        return;
    }

    @Override
    public MarkerOperator copy() {
        MarkerOperator operator = new MarkerOperator();
        for (Map.Entry<Class<?>, Object> entry : attributes.entrySet()) {
            Class<?> key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof OperatorAttribute) {
                value = ((OperatorAttribute) value).copy();
                assert key.isInstance(value);
            }
            operator.attributes.put(key, value);
        }
        return copyAttributesTo(operator);
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
        return getInputs().get(0);
    }

    /**
     * Returns the a single operator output of this.
     * @return the operator output
     */
    public OperatorOutput getOutput() {
        assert getOutputs().size() == 1;
        return getOutputs().get(0);
    }

    /**
     * Returns the all attribute types which this operator has.
     * @return the all attribute types
     */
    public Set<Class<?>> getAttributeTypes() {
        return Collections.unmodifiableSet(attributes.keySet());
    }

    /**
     * Returns an attribute.
     * @param <T> the attribute type
     * @param attributeType the attribute type
     * @return the attribute value, or {@code null} if the operator has no such an attribute
     */
    public <T> T getAttribute(Class<T> attributeType) {
        Object value = attributes.get(attributeType);
        if (value == null) {
            return null;
        } else {
            return attributeType.cast(value);
        }
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
     */
    public static final class Builder {

        private final MarkerOperator owner;

        Builder(MarkerOperator owner) {
            this.owner = owner;
        }

        /**
         * Adds an attribute to the building operator.
         * When clients {@link MarkerOperator#copy() copy operators},
         * only attributes implementing {@link OperatorAttribute}
         * are also copied using {@link OperatorAttribute#copy()} method.
         * @param attributeType attribute type
         * @param attributeValue attribute value
         * @param <T> attribute type
         * @return this
         */
        public <T> Builder attribute(Class<T> attributeType, T attributeValue) {
            if (attributeValue == null) {
                throw new IllegalArgumentException("attribute value must not be null"); //$NON-NLS-1$
            } else {
                owner.attributes.put(attributeType, attributeValue);
            }
            return this;
        }

        /**
         * Returns the built operator.
         * @return the built operator
         */
        public MarkerOperator build() {
            return owner;
        }
    }
}
