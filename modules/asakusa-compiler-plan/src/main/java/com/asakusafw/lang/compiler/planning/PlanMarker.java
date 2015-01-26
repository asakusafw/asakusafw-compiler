package com.asakusafw.lang.compiler.planning;

import com.asakusafw.lang.compiler.model.graph.OperatorAttribute;

/**
 * Represents a planning hint.
 */
public enum PlanMarker implements OperatorAttribute {

    /**
     * beginning of the operator graph.
     */
    BEGIN,

    /**
     * ending of the operator graph.
     */
    END,

    /**
     * required checkpoint operation on the marked location.
     */
    CHECKPOINT,

    /**
     * gathering operation is on the next of marked location.
     */
    GATHER,

    /**
     * broadcast operation is on the next of marked location.
     */
    BROADCAST,
    ;

    @Override
    public PlanMarker copy() {
        return this;
    }
}
