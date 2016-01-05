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
package com.asakusafw.lang.compiler.model.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;

/**
 * Represents an operator vertex.
 * @see OperatorAttribute
 * @see Operators
 * @since 0.1.0
 * @version 0.3.0
 */
public abstract class Operator {

    private static final AtomicLong COUNTER = new AtomicLong();

    private final long serialNumber = COUNTER.getAndIncrement();

    private long originalSerialNumber = serialNumber;

    final List<OperatorProperty> properties = new ArrayList<>();

    final Map<Class<?>, Object> attributes = new LinkedHashMap<>();

    final Set<OperatorConstraint> constraints = EnumSet.noneOf(OperatorConstraint.class);

    /**
     * Creates a new instance.
     */
    protected Operator() {
        return;
    }

    /**
     * Returns the operator kind.
     * @return the operator kind
     */
    public abstract OperatorKind getOperatorKind();

    /**
     * Returns the serial number of this operator.
     * @return the serial number
     */
    public long getSerialNumber() {
        return serialNumber;
    }

    /**
     * Returns the original serial number of this operator.
     * If this operator is a {@link #copy() copy} of an other operator,
     * the original serial number is as same as the other operator's one.
     * Otherwise, if this operator is original (not a copy), the original serial number is
     * as same to the {@link #getSerialNumber() normal serial number}.
     * @return the originalSerialNumber
     */
    public long getOriginalSerialNumber() {
        return originalSerialNumber;
    }

    /**
     * Returns a copy of this operator.
     * The copy is not connected to any neighbors.
     * If this operator has operator graphs recursively, this performs as 'deep-copy'.
     * @return the created copy
     */
    public abstract Operator copy();

    /**
     * Copies operator attributes into the target operator.
     * @param copy the target operator
     * @param <T> the target operator type
     * @return the target operator
     */
    protected final <T extends Operator> T copyAttributesTo(T copy) {
        ((Operator) copy).originalSerialNumber = originalSerialNumber;
        for (OperatorProperty property : properties) {
            switch (property.getPropertyKind()) {
            case INPUT: {
                OperatorInput p = (OperatorInput) property;
                copy.properties.add(new OperatorInput(copy, p.getName(), p.getDataType(), p.getGroup()));
                break;
            }
            case OUTPUT: {
                OperatorOutput p = (OperatorOutput) property;
                copy.properties.add(new OperatorOutput(copy, p.getName(), p.getDataType()));
                break;
            }
            case ARGUMENT: {
                OperatorArgument p = (OperatorArgument) property;
                copy.properties.add(new OperatorArgument(p.getName(), p.getValue()));
                break;
            }
            default:
                throw new AssertionError(property);
            }
        }
        for (Map.Entry<Class<?>, Object> entry : attributes.entrySet()) {
            Class<?> key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof OperatorAttribute) {
                value = ((OperatorAttribute) value).copy();
                assert key.isInstance(value);
            }
            copy.attributes.put(key, value);
        }
        copy.constraints.addAll(constraints);
        return copy;
    }

    /**
     * Disconnects from all neighbor operators.
     * @return this
     */
    public Operator disconnectAll() {
        for (OperatorPort port : getProperties(OperatorPort.class)) {
            port.disconnectAll();
        }
        return this;
    }

    /**
     * Returns the properties of this operator.
     * @return the properties
     */
    public List<? extends OperatorProperty> getProperties() {
        return Collections.unmodifiableList(properties);
    }

    /**
     * Returns the operator inputs.
     * @return the operator inputs
     */
    public List<OperatorInput> getInputs() {
        return getProperties(OperatorInput.class);
    }

    /**
     * Returns the operator outputs.
     * @return the operator outputs
     */
    public List<OperatorOutput> getOutputs() {
        return getProperties(OperatorOutput.class);
    }

    /**
     * Returns the operator arguments.
     * @return the operator arguments
     */
    public List<OperatorArgument> getArguments() {
        return getProperties(OperatorArgument.class);
    }

    /**
     * Returns the operator constraints.
     * @return the operator constraints
     */
    public Set<OperatorConstraint> getConstraints() {
        return Collections.unmodifiableSet(constraints);
    }

    /**
     * Returns an operator input.
     * @param name the input name
     * @return the found element, or {@code null} it is not found
     */
    public OperatorInput findInput(String name) {
        return findProperty(OperatorInput.class, name);
    }

    /**
     * Returns an operator output.
     * @param name the output name
     * @return the found element, or {@code null} it is not found
     */
    public OperatorOutput findOutput(String name) {
        return findProperty(OperatorOutput.class, name);
    }

    /**
     * Returns an operator argument.
     * @param name the argument name
     * @return the found element, or {@code null} it is not found
     */
    public OperatorArgument findArgument(String name) {
        return findProperty(OperatorArgument.class, name);
    }

    private <T> T findProperty(Class<T> type, String name) {
        for (OperatorProperty property : getProperties()) {
            if (property.getName().equals(name) && type.isInstance(property)) {
                return type.cast(property);
            }
        }
        return null;
    }

    private <T> List<T> getProperties(Class<T> type) {
        List<T> results = new ArrayList<>();
        for (OperatorProperty property : getProperties()) {
            if (type.isInstance(property)) {
                results.add(type.cast(property));
            }
        }
        return results;
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

    /**
     * Represents a kind of {@link Operator}.
     * @since 0.1.0
     * @version 0.3.0
     */
    public enum OperatorKind {

        /**
         * Core operators.
         * @see CoreOperator
         */
        CORE,

        /**
         * User operators.
         * @see UserOperator
         */
        USER,

        /**
         * Nested operators.
         * @see FlowOperator
         */
        FLOW,

        /**
         * External inputs.
         * @see ExternalInput
         */
        INPUT,

        /**
         * External outputs.
         * @see ExternalOutput
         */
        OUTPUT,

        /**
         * Pseudo operators for information.
         * This will be used only in planning, and must not appear in DSLs.
         * @see MarkerOperator
         */
        MARKER,

        /**
         * Custom operators.
         * This will be appeared in some optimization phases.
         * @see CustomOperator
         * @since 0.3.0
         */
        CUSTOM,
    }

    /**
     * The base implementation of operator builders.
     * @param <TOperator> the operator type
     * @param <TSelf> the actual builder type
     * @since 0.3.0
     */
    public abstract static class BuilderBase<TOperator extends Operator, TSelf> {

        private final TOperator owner;

        /**
         * Creates a new instance.
         * @param owner the target operator
         */
        protected BuilderBase(TOperator owner) {
            this.owner = owner;
        }

        /**
         * Returns the building operator.
         * @return the building operator
         */
        protected TOperator getOwner() {
            return owner;
        }

        /**
         * Returns this object.
         * @return this
         */
        protected abstract TSelf getSelf();

        /**
         * Returns the built operator.
         * @return the built operator
         */
        public TOperator build() {
            return owner;
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
        public <T> TSelf attribute(Class<T> attributeType, T attributeValue) {
            Objects.requireNonNull(attributeType, "attributeType must not be null"); //$NON-NLS-1$
            Objects.requireNonNull(attributeValue, "attributeValue must not be null"); //$NON-NLS-1$
            owner.attributes.put(attributeType, attributeValue);
            return getSelf();
        }
    }

    /**
     * An abstract implementation of operator builders.
     * @param <TOperator> the operator type
     * @param <TSelf> the actual builder type
     * @since 0.1.0
     * @version 0.3.0
     */
    public abstract static class AbstractBuilder<TOperator extends Operator, TSelf>
            extends BuilderBase<TOperator, TSelf> {

        /**
         * Creates a new instance.
         * @param owner the target operator
         */
        protected AbstractBuilder(TOperator owner) {
            super(owner);
        }

        /**
         * Adds an input port to the building operator.
         * @param name the port name
         * @param dataType the data type on the port
         * @param upstreams the optional upstream ports to connect to the created input
         * @return this
         */
        public TSelf input(String name, TypeDescription dataType, OperatorOutput... upstreams) {
            return input(name, dataType, (Group) null, upstreams);
        }

        /**
         * Adds an input port to the building operator.
         * @param name the port name
         * @param dataType the data type on the port
         * @param group the grouping instruction
         * @param upstreams the optional upstream ports to connect to the created input
         * @return this
         * @see Groups
         */
        public TSelf input(String name, TypeDescription dataType, Group group, OperatorOutput... upstreams) {
            TOperator owner = getOwner();
            OperatorInput port = new OperatorInput(owner, name, dataType, group);
            owner.properties.add(port);
            for (OperatorOutput upstream : upstreams) {
                port.connect(upstream);
            }
            return getSelf();
        }

        /**
         * Adds an input port to the building operator.
         * @param name the port name
         * @param upstream the mandatory upstream port to connect to the created input
         * @param upstreams the optional upstream ports to connect to the created input
         * @return this
         */
        public TSelf input(String name, OperatorOutput upstream, OperatorOutput... upstreams) {
            return input(name, (Group) null, upstream, upstreams);
        }

        /**
         * Adds an input port to the building operator.
         * @param name the port name
         * @param group the grouping instruction
         * @param upstream the mandatory upstream port to connect to the created input
         * @param upstreams the optional upstream ports to connect to the created input
         * @return this
         * @see Groups
         */
        public TSelf input(String name, Group group, OperatorOutput upstream, OperatorOutput... upstreams) {
            OperatorOutput[] joint = new OperatorOutput[upstreams.length + 1];
            joint[0] = upstream;
            System.arraycopy(upstreams, 0, joint, 1, upstreams.length);
            return input(name, upstream.getDataType(), group, joint);
        }

        /**
         * Adds an output port to the building operator.
         * @param name the port name
         * @param dataType the data type on the port
         * @return this
         */
        public TSelf output(String name, TypeDescription dataType) {
            TOperator owner = getOwner();
            owner.properties.add(new OperatorOutput(owner, name, dataType));
            return getSelf();
        }

        /**
         * Adds an argument to the building operator.
         * @param name the argument name
         * @param value the argument value
         * @return this
         */
        public TSelf argument(String name, ValueDescription value) {
            TOperator owner = getOwner();
            owner.properties.add(new OperatorArgument(name, value));
            return getSelf();
        }

        /**
         * Adds constraints to the building operator.
         * @param constraints the constraints
         * @return this
         */
        public TSelf constraint(Collection<OperatorConstraint> constraints) {
            TOperator owner = getOwner();
            owner.constraints.addAll(constraints);
            return getSelf();
        }

        /**
         * Adds constraints to the building operator.
         * @param constraints the constraints
         * @return this
         */
        public TSelf constraint(OperatorConstraint... constraints) {
            TOperator owner = getOwner();
            Collections.addAll(owner.constraints, constraints);
            return getSelf();
        }
    }
}
