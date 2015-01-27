package com.asakusafw.lang.compiler.planning;

import java.util.Set;

import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;

/**
 * Represents a logical execution unit of {@link Plan}.
 * TODO invariants
 */
public interface SubPlan extends AttributeContainer {

    /**
     * Returns the owner of this sub-plan.
     * @return the owner
     */
    Plan getOwner();

    /**
     * Returns the input ports of this sub-plan.
     * @return the input ports
     */
    Set<? extends Input> getInputs();

    /**
     * Returns the output ports of this sub-plan.
     * @return the output ports
     */
    Set<? extends Output> getOutputs();

    /**
     * Returns operators in this sub-plan.
     * @return the operators (includes input/output operators)
     */
    Set<? extends Operator> getOperators();

    /**
     * Returns the input port of this sub-plan for the operator.
     * @param operator the operator
     * @return the input port which original operator is the specified one
     */
    Input findInput(Operator operator);

    /**
     * Returns the output port of this sub-plan for the operator.
     * @param operator the operator
     * @return the output port which original operator is the specified one
     */
    Output findOutput(Operator operator);

    /**
     * Represents an input/output port of {@link SubPlan}.
     */
    public interface Port extends AttributeContainer {

        /**
         * Returns the owner of this port.
         * @return the owner
         */
        SubPlan getOwner();

        /**
         * Returns the connected opposite ports.
         * @return the opposite ports
         */
        Set<? extends Port> getOpposites();

        /**
         * Returns the operator which represents this port.
         * The returning operator will detached from other sub-plan.
         * @return the original operator
         */
        MarkerOperator getOperator();
    }

    /**
     * Represents an input port of {@link SubPlan}.
     */
    public interface Input extends Port {

        /**
         * Returns the upstream inputs.
         */
        @Override
        Set<? extends Output> getOpposites();
    }

    /**
     * Represents an input port of {@link SubPlan}.
     */
    public interface Output extends Port {

        /**
         * Returns the downstream inputs.
         */
        @Override
        Set<? extends Input> getOpposites();
    }
}
