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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents an operator output port.
 * @since 0.1.0
 * @version 0.4.1
 */
public class OperatorOutput implements OperatorPort {

    private final Operator owner;

    private final String name;

    private final TypeDescription dataType;

    private final AttributeMap attributes;

    private final Set<OperatorInput> opposites = new HashSet<>();

    /**
     * Creates a new instance.
     * @param owner the owner of this port
     * @param name the port name
     * @param dataType the data type
     */
    public OperatorOutput(Operator owner, String name, TypeDescription dataType) {
        this(owner, name, dataType, AttributeMap.EMPTY);
    }

    /**
     * Creates a new instance.
     * @param owner the owner of this port
     * @param name the port name
     * @param dataType the data type
     * @param attributes the port attributes
     * @since 0.4.1
     */
    public OperatorOutput(Operator owner, String name, TypeDescription dataType, AttributeMap attributes) {
        this.owner = owner;
        this.name = name;
        this.dataType = dataType;
        this.attributes = attributes;
    }

    @Override
    public final PropertyKind getPropertyKind() {
        return PropertyKind.OUTPUT;
    }

    @Override
    public Operator getOwner() {
        return owner;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TypeDescription getDataType() {
        return dataType;
    }

    @Override
    public Set<Class<?>> getAttributeTypes() {
        return attributes.getAttributeTypes();
    }

    @Override
    public <T> T getAttribute(Class<T> attributeType) {
        return attributes.getAttribute(attributeType);
    }

    AttributeMap getAttributeMap() {
        return attributes;
    }

    /**
     * Returns whether this is connected to the specified port.
     * @param oppsite the target port
     * @return {@code true} if they are connected, otherwise {@code false}
     */
    public boolean isConnected(OperatorInput oppsite) {
        return opposites.contains(oppsite);
    }

    /**
     * Connects to the downstream port.
     * If the opposite port is already connected, this will do nothing.
     * @param opposite the opposite port
     */
    public void connect(OperatorInput opposite) {
        this.connect0(opposite);
        opposite.connect0(this);
    }

    /**
     * Disconnect from the downstream port.
     * If the opposite port is not connected, this will do nothing.
     * @param opposite the opposite port
     */
    public void disconnect(OperatorInput opposite) {
        this.disconnect0(opposite);
        opposite.disconnect0(this);
    }

    void connect0(OperatorInput opposite) {
        opposites.add(opposite);
    }

    void disconnect0(OperatorInput opposite) {
        opposites.remove(opposite);
    }

    @Override
    public void disconnectAll() {
        for (OperatorInput port : opposites) {
            port.disconnect0(this);
        }
        opposites.clear();
    }

    @Override
    public boolean hasOpposites() {
        return opposites.isEmpty() == false;
    }
    /**
     * Returns whether this and the target port has the same opposites.
     * @param other the target port
     * @return {@code true} if the both has the same opposites, otherwise {@code false}
     */
    public boolean hasSameOpposites(OperatorOutput other) {
        return opposites.equals(other.opposites);
    }

    @Override
    public Collection<OperatorInput> getOpposites() {
        return Collections.unmodifiableList(new ArrayList<>(opposites));
    }

    OperatorOutput copy(Operator newOwner) {
        return new OperatorOutput(newOwner, name, dataType, attributes.copy());
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Output({0})", //$NON-NLS-1$
                name);
    }
}
