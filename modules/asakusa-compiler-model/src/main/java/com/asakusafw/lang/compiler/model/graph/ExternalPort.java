package com.asakusafw.lang.compiler.model.graph;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents an external/flow I/O port.
 */
public abstract class ExternalPort extends Operator {

    /**
     * Returns the port kind.
     * @return the port kind
     */
    public abstract PortKind getPortKind();

    /**
     * Returns the port name.
     * Each port name is identical in input ports or output ports in the same {@link OperatorGraph}.
     * @return the port name
     */
    public abstract String getName();

    /**
     * Returns the operator port.
     * @return the operator port
     */
    public abstract OperatorPort getOperatorPort();

    /**
     * Returns the data type on this port.
     * @return the data type
     */
    public TypeDescription getDataType() {
        return getOperatorPort().getDataType();
    }

    /**
     * Returns whether this port is external or not.
     * @return {@code true} if this is external, otherwise {@code false}
     */
    public boolean isExternal() {
        return getDescriptionClass() != null;
    }

    /**
     * Returns the importer/exporter description class.
     * @return the importer/exporter description class, or {@code null} if this represents a flow I/O
     */
    public abstract ClassDescription getDescriptionClass();

    /**
     * Represents a kind of port.
     */
    public static enum PortKind {

        /**
         * input port.
         * @see ExternalInput
         */
        INPUT,

        /**
         * output port.
         * @see ExternalOutput
         */
        OUTPUT,
    }
}
