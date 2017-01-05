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
import java.util.function.Consumer;

import com.asakusafw.lang.compiler.common.AttributeEntry;
import com.asakusafw.lang.compiler.common.AttributeProvider;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;

/**
 * Represents an operator vertex.
 * @see OperatorAttribute
 * @see Operators
 * @since 0.1.0
 * @version 0.4.1
 */
public abstract class Operator implements AttributeProvider {

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
            case INPUT:
                copy.properties.add(((OperatorInput) property).copy(copy));
                break;
            case OUTPUT:
                copy.properties.add(((OperatorOutput) property).copy(copy));
                break;
            case ARGUMENT:
                copy.properties.add(property);
                break;
            default:
                throw new AssertionError(property);
            }
        }
        AttributeMap.copyTo(attributes, copy.attributes);
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
     * Returns the {@code index}-th operator input.
     * @param index the target index
     * @return the operator input
     * @throws IndexOutOfBoundsException if the given index is out of bounds
     * @since 0.4.1
     */
    public OperatorInput getInput(int index) {
        return getProperty(OperatorInput.class, index);
    }

    /**
     * Returns the {@code index}-th operator output.
     * @param index the target index
     * @return the operator output
     * @throws IndexOutOfBoundsException if the given index is out of bounds
     * @since 0.4.1
     */
    public OperatorOutput getOutput(int index) {
        return getProperty(OperatorOutput.class, index);
    }

    /**
     * Returns the {@code index}-th operator argument.
     * @param index the target index
     * @return the operator argument
     * @throws IndexOutOfBoundsException if the given index is out of bounds
     * @since 0.4.1
     */
    public OperatorArgument getArgument(int index) {
        return getProperty(OperatorArgument.class, index);
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

    private <T> T getProperty(Class<T> type, int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        int current = 0;
        for (OperatorProperty property : getProperties()) {
            if (type.isInstance(property)) {
                if (current == index) {
                    return type.cast(property);
                } else {
                    current++;
                }
            }
        }
        throw new IndexOutOfBoundsException();
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

    @Override
    public Set<Class<?>> getAttributeTypes() {
        return Collections.unmodifiableSet(attributes.keySet());
    }

    /**
     * Returns an attribute.
     * @param <T> the attribute type
     * @param attributeType the attribute type
     * @return the attribute value, or {@code null} if the operator has no such an attribute
     */
    @Override
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
        /**
         * Adds an attribute to the building operator.
         * @param value the attribute value
         * @return this
         * @since 0.4.1
         */
        public TSelf attribute(Enum<?> value) {
            owner.attributes.put(value.getDeclaringClass(), value);
            return getSelf();
        }

        /**
         * Adds an attribute to the building operator.
         * When clients {@link MarkerOperator#copy() copy operators},
         * only attributes implementing {@link OperatorAttribute}
         * are also copied using {@link OperatorAttribute#copy()} method.
         * @param entry the attribute entry
         * @return this
         * @since 0.4.1
         */
        public TSelf attribute(AttributeEntry<?> entry) {
            owner.attributes.put(entry.getType(), entry.getValue());
            return getSelf();
        }
    }

    /**
     * An abstract implementation of operator builders.
     * @param <TOperator> the operator type
     * @param <TSelf> the actual builder type
     * @since 0.1.0
     * @version 0.4.1
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
         * Adds a clone of the given input port to the building operator.
         * @param port the original input
         * @return this
         * @since 0.4.1
         */
        public TSelf input(OperatorInput port) {
            return input(port, c -> {
                return;
            });
        }

        /**
         * Adds a clone of the given input port to the building operator.
         * @param port the original input
         * @param configurator the configurator
         * @return this
         * @since 0.4.1
         */
        public TSelf input(OperatorInput port, Consumer<InputOptionBuilder> configurator) {
            return input(port.getName(), port.getDataType(), c -> {
                c.unit(port.getInputUnit());
                c.group(port.getGroup());
                port.getAttributeMap().copyTo(c.attributes);
                configurator.accept(c);
            });
        }

        /**
         * Adds a clone of the given output port to the building operator.
         * @param port the original output
         * @return this
         * @since 0.4.1
         */
        public TSelf output(OperatorOutput port) {
            TOperator owner = getOwner();
            owner.properties.add(port.copy(owner));
            return getSelf();
        }

        /**
         * Adds a clone of the given output port to the building operator.
         * @param port the original output
         * @param configurator the configurator
         * @return this
         * @since 0.4.1
         */
        public TSelf output(OperatorOutput port, Consumer<OutputOptionBuilder> configurator) {
            return output(port.getName(), port.getDataType(), c -> {
                port.getAttributeMap().copyTo(c.attributes);
                configurator.accept(c);
            });
        }

        /**
         * Adds a clone of the given argument to the building operator.
         * @param argument the original argument
         * @return this
         * @since 0.4.1
         */
        public TSelf argument(OperatorArgument argument) {
            TOperator owner = getOwner();
            owner.properties.add(argument);
            return getSelf();
        }

        /**
         * Adds an input port to the building operator.
         * @param name the port name
         * @param dataType the data type on the port
         * @param configurator configurator of the building inputs
         * @return this
         * @since 0.4.1
         */
        public TSelf input(String name, TypeDescription dataType, Consumer<InputOptionBuilder> configurator) {
            InputOptionBuilder sub = new InputOptionBuilder();
            configurator.accept(sub);
            TOperator owner = getOwner();
            OperatorInput port = new OperatorInput(
                    owner, name, dataType,
                    sub.inputUnit, sub.group,
                    sub.attributes.build());
            owner.properties.add(port);
            for (OperatorOutput upstream : sub.upstreams) {
                port.connect(upstream);
            }
            return getSelf();
        }

        /**
         * Adds an input port to the building operator.
         * @param name the port name
         * @param dataType the data type on the port
         * @param configurator configurator of the building inputs
         * @return this
         * @since 0.4.1
         */
        public TSelf output(String name, TypeDescription dataType, Consumer<OutputOptionBuilder> configurator) {
            OutputOptionBuilder sub = new OutputOptionBuilder();
            configurator.accept(sub);
            TOperator owner = getOwner();
            OperatorOutput port = new OperatorOutput(owner, name, dataType, sub.attributes.build());
            owner.properties.add(port);
            return getSelf();
        }

        /**
         * Adds an input port to the building operator.
         * @param name the port name
         * @param dataType the data type on the port
         * @param upstreams the optional upstream ports to connect to the created input
         * @return this
         */
        public TSelf input(String name, TypeDescription dataType, OperatorOutput... upstreams) {
            return input(name, dataType, c -> c.upstreams(upstreams));
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
            return input(name, dataType, c -> c.group(group).upstreams(upstreams));
        }

        /**
         * Adds an input port to the building operator.
         * @param name the port name
         * @param upstream the mandatory upstream port to connect to the created input
         * @param upstreams the optional upstream ports to connect to the created input
         * @return this
         */
        public TSelf input(String name, OperatorOutput upstream, OperatorOutput... upstreams) {
            TypeDescription dataType = upstream.getDataType();
            return input(name, dataType, c -> c.upstream(upstream).upstreams(upstreams));
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
            TypeDescription dataType = upstream.getDataType();
            return input(name, dataType, c -> c.group(group).upstream(upstream).upstreams(upstreams));
        }

        /**
         * Adds an output port to the building operator.
         * @param name the port name
         * @param dataType the data type on the port
         * @return this
         */
        public TSelf output(String name, TypeDescription dataType) {
            return output(name, dataType, c -> {
                return;
            });
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

    /**
     * An abstract option builder for {@link OperatorPort}.
     * @since 0.4.1
     */
    public interface PortOptionBuilder {

        /**
         * Adds an attribute to the building input.
         * @param entry the attribute entry
         * @return this
         */
        PortOptionBuilder attribute(AttributeEntry<?> entry);

        /**
         * Adds an attribute to the building input.
         * @param value the attribute value
         * @return this
         */
        PortOptionBuilder attribute(Enum<?> value);

        /**
         * Adds an attribute to the building input.
         * @param <T> the attribute type
         * @param type the attribute type
         * @param value the attribute value
         * @return this
         */
        <T> PortOptionBuilder attribute(Class<T> type, T value);
    }

    /**
     * A option builder for {@link OperatorInput}.
     * @since 0.4.1
     */
    public static class InputOptionBuilder implements PortOptionBuilder {

        OperatorInput.InputUnit inputUnit = OperatorInput.InputUnit.RECORD;

        Group group;

        final List<OperatorOutput> upstreams = new ArrayList<>();

        final AttributeMap.Builder attributes = new AttributeMap.Builder();

        InputOptionBuilder() {
            return;
        }

        /**
         * Sets the input unit kind.
         * @param unit the input unit kind
         * @return this
         */
        public InputOptionBuilder unit(OperatorInput.InputUnit unit) {
            this.inputUnit = unit;
            return this;
        }

        /**
         * Sets the grouping information.
         * If the {@link #unit(com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit) unit()} has not been
         * set, this operation will change the input unit to
         * {@link com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit#GROUP GROUP}.
         * @param grouping grouping information
         * @return this
         */
        public InputOptionBuilder group(Group grouping) {
            this.group = grouping;
            if (grouping != null && inputUnit == OperatorInput.InputUnit.RECORD) {
                inputUnit = OperatorInput.InputUnit.GROUP;
            }
            return this;
        }

        /**
         * Adds an upstream of the building input.
         * @param upstream the upstream port
         * @return this
         */
        public InputOptionBuilder upstream(OperatorOutput upstream) {
            this.upstreams.add(upstream);
            return this;
        }

        /**
         * Adds upstreams of the building input.
         * @param upstreamArray the upstream port
         * @return this
         */
        public InputOptionBuilder upstreams(OperatorOutput... upstreamArray) {
            Collections.addAll(this.upstreams, upstreamArray);
            return this;
        }

        @Override
        public InputOptionBuilder attribute(Enum<?> value) {
            this.attributes.add(value);
            return this;
        }

        @Override
        public InputOptionBuilder attribute(AttributeEntry<?> entry) {
            this.attributes.add(entry);
            return this;
        }

        @Override
        public <T> InputOptionBuilder attribute(Class<T> type, T value) {
            this.attributes.add(type, value);
            return this;
        }
    }

    /**
     * A option builder for {@link OperatorOutput}.
     * @since 0.4.1
     */
    public static class OutputOptionBuilder implements PortOptionBuilder {

        final AttributeMap.Builder attributes = new AttributeMap.Builder();

        OutputOptionBuilder() {
            return;
        }

        @Override
        public OutputOptionBuilder attribute(Enum<?> value) {
            this.attributes.add(value);
            return this;
        }

        @Override
        public OutputOptionBuilder attribute(AttributeEntry<?> entry) {
            this.attributes.add(entry);
            return this;
        }

        @Override
        public <T> OutputOptionBuilder attribute(Class<T> type, T value) {
            this.attributes.add(type, value);
            return this;
        }
    }
}
