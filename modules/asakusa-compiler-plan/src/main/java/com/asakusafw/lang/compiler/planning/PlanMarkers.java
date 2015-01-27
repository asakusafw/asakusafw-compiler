package com.asakusafw.lang.compiler.planning;

import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.Operators;

/**
 * Utilities for {@link PlanMarker}.
 * @see Operators
 */
public final class PlanMarkers {

    private PlanMarkers() {
        return;
    }

    /**
     * Creates a new instance with the specified {@link PlanMarker} kind.
     * @param dataType the data type
     * @param marker the marker kind
     * @return the created operator
     */
    public static MarkerOperator newInstance(TypeDescription dataType, PlanMarker marker) {
        return MarkerOperator.builder(dataType)
                .attribute(PlanMarker.class, marker)
                .build();
    }

    /**
     * Returns a {@link PlanMarker} of the operator.
     * @param operator the target operator
     * @return the plan marker of the operator, or {@code null} if it has no plan markers
     */
    public static PlanMarker get(Operator operator) {
        if (operator.getOperatorKind() == OperatorKind.MARKER) {
            return ((MarkerOperator) operator).getAttribute(PlanMarker.class);
        }
        return null;
    }

    /**
     * Inserts a new plan marker into the target input port.
     * @param marker the plan marker kind
     * @param port the target port
     * @return the created marker
     */
    public static MarkerOperator insert(PlanMarker marker, OperatorInput port) {
        return Operators.insert(newInstance(port.getDataType(), marker), port);
    }

    /**
     * Inserts a new plan marker into the target output port.
     * @param marker the plan marker kind
     * @param port the target port
     * @return the created marker
     */
    public static MarkerOperator insert(PlanMarker marker, OperatorOutput port) {
        return Operators.insert(newInstance(port.getDataType(), marker), port);
    }

    /**
     * Inserts a new plan marker into the connection between the specified ports.
     * @param marker the plan marker kind
     * @param upstream the upstream port
     * @param downstream the downstream port
     * @return the created marker
     */
    public static MarkerOperator insert(PlanMarker marker, OperatorOutput upstream, OperatorInput downstream) {
        assert upstream.getDataType().equals(downstream.getDataType());
        assert upstream.isConnected(downstream);
        return Operators.insert(newInstance(upstream.getDataType(), marker), upstream, downstream);
    }
}
