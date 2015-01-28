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
 */
public class OperatorOutput implements OperatorPort {

    private final Operator owner;

    private final String name;

    private final TypeDescription dataType;

    private final Set<OperatorInput> opposites = new HashSet<>();

    /**
     * Creates a new instance.
     * @param owner the owner of this port
     * @param name the port name
     * @param dataType the data type
     */
    public OperatorOutput(Operator owner, String name, TypeDescription dataType) {
        this.owner = owner;
        this.name = name;
        this.dataType = dataType;
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

    @Override
    public Collection<OperatorInput> getOpposites() {
        return Collections.unmodifiableList(new ArrayList<>(opposites));
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Output({0})",
                name);
    }
}
