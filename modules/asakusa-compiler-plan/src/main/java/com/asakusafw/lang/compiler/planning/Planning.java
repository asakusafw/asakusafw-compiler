package com.asakusafw.lang.compiler.planning;

import java.util.Map;

import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;

/**
 * Utilities for execution planning.
 * @see Operators
 */
public final class Planning {

    private Planning() {
        return;
    }

    /**
     * Normalizes the operator graph.
     * This includes following operations:
     * <ol>
     * <li> flatten operators (unnest {@link FlowOperator}s, and remove non-external inputs/outputs) </li>
     * <li> add a virtual input port for {@link ExternalInput}s </li>
     * <li> add a virtual output port for {@link ExternalOutput}s </li>
     * <li> add {@link OperatorConstraint#AT_LEAST_ONCE at-least-once constraint} to {@link ExternalOutput}s </li>
     * <li> add {@link PlanMarker#BEGIN BEGIN} markers to input ports without any opposites </li>
     * <li> add {@link PlanMarker#END END} markers to output ports without any opposites </li>
     * </ol>
     * This will directly modify the specified operator graph.
     * @param graph the target operator graph
     * @see OperatorGraph#copy()
     */
    public static void normalize(OperatorGraph graph) {
        flatten(graph);
        enhanceExternalPorts(graph);
        insertTerminatorsForPorts(graph);
    }

    static void flatten(OperatorGraph graph) {
        graph.rebuild();
        for (Operator operator : graph.getOperators()) {
            if (operator.getOperatorKind() == OperatorKind.FLOW) {
                flatten0((FlowOperator) operator);
                graph.remove(operator);
            }
        }
    }

    private static void flatten0(FlowOperator flow) {
        OperatorGraph graph = flow.getOperatorGraph().rebuild();

        // redirect flow I/Os
        Map<String, ExternalInput> inputs = graph.getInputs();
        for (OperatorInput outer : flow.getInputs()) {
            ExternalInput inner = inputs.get(outer.getName());
            assert inner.isExternal() == false;
            Operators.connectAll(outer.getOpposites(), inner.getOperatorPort().getOpposites());
            inner.disconnectAll();
            outer.disconnectAll();
        }
        Map<String, ExternalOutput> outputs = graph.getOutputs();
        for (OperatorOutput outer : flow.getOutputs()) {
            ExternalOutput inner = outputs.get(outer.getName());
            assert inner.isExternal() == false;
            Operators.connectAll(inner.getOperatorPort().getOpposites(), outer.getOpposites());
            inner.disconnectAll();
            outer.disconnectAll();
        }
        flow.disconnectAll();

        // remove flow I/Os
        for (ExternalInput port : inputs.values()) {
            if (port.isExternal()) {
                continue;
            }
            port.disconnectAll();
            graph.remove(port);
        }
        for (ExternalOutput port : outputs.values()) {
            if (port.isExternal()) {
                continue;
            }
            port.disconnectAll();
            graph.remove(port);
        }

        // flatten recursively
        for (Operator operator : graph.getOperators()) {
            if (operator.getOperatorKind() == OperatorKind.FLOW) {
                flatten0((FlowOperator) operator);
                graph.remove(operator);
            }
        }
    }

    private static void enhanceExternalPorts(OperatorGraph graph) {
        graph.rebuild();
        for (ExternalInput port : graph.getInputs().values()) {
            if (port.getInputs().isEmpty() == false) {
                continue;
            }
            OperatorOutput orig = port.getOperatorPort();
            ExternalInput enhanced = ExternalInput.builder(port.getName(), port.getDescriptionClass())
                    .input(ExternalInput.PORT_NAME, port.getDataType()) // virtual
                    .output(orig.getName(), orig.getDataType())
                    .constraint(port.getConstraints())
                    .build();
            Operators.connectAll(enhanced.getOperatorPort(), orig.getOpposites());
            graph.remove(port.disconnectAll());
            graph.add(enhanced);
        }
        for (ExternalOutput port : graph.getOutputs().values()) {
            if (port.getOutputs().isEmpty() == false) {
                continue;
            }
            OperatorInput orig = port.getOperatorPort();
            ExternalOutput enhanced = ExternalOutput.builder(port.getName(), port.getDescriptionClass())
                    .input(orig.getName(), orig.getDataType(), orig.getGroup())
                    .output(ExternalOutput.PORT_NAME, orig.getDataType()) // virtual
                    .constraint(port.getConstraints())
                    .constraint(OperatorConstraint.AT_LEAST_ONCE)
                    .build();
            Operators.connectAll(orig.getOpposites(), enhanced.getOperatorPort());
            graph.remove(port.disconnectAll());
            graph.add(enhanced);
        }
    }

    private static void insertTerminatorsForPorts(OperatorGraph graph) {
        graph.rebuild();
        for (Operator operator : graph.getOperators()) {
            if (operator.getOperatorKind() == OperatorKind.MARKER) {
                continue;
            }
            for (OperatorInput port : operator.getInputs()) {
                if (port.getOpposites().isEmpty()) {
                    MarkerOperator marker = PlanMarkers.insert(PlanMarker.BEGIN, port);
                    graph.add(marker);
                }
            }
            for (OperatorOutput port : operator.getOutputs()) {
                if (port.getOpposites().isEmpty()) {
                    MarkerOperator marker = PlanMarkers.insert(PlanMarker.END, port);
                    graph.add(marker);
                }
            }
        }
    }
}
