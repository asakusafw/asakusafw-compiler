package com.asakusafw.lang.compiler.model.graph;

import java.util.Collection;

import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents an I/O port of operator.
 */
public interface OperatorPort extends OperatorProperty {

    /**
     * Returns the declaring operator.
     * @return the declaring operator
     */
    Operator getOwner();

    /**
     * Returns the port name.
     * @return the port name
     */
    @Override
    String getName();

    /**
     * Returns the data type on this port.
     * @return the data type
     */
    TypeDescription getDataType();

    /**
     * Disconnects from all opposite ports.
     */
    void disconnectAll();

    /**
     * Returns whether this port has at least one opposite or not.
     * @return {@code true} if this port has any opposites
     */
    boolean hasOpposites();

    /**
     * Returns the opposite ports.
     * @return the opposite ports
     */
    Collection<? extends OperatorPort> getOpposites();
}
