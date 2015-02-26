package com.asakusafw.lang.compiler.analyzer.util;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.graph.Operator;

/**
 * Generic operator utilities.
 */
public final class OperatorUtil {

    private OperatorUtil() {
        return;
    }

    /**
     * Validates operator ports.
     * @param operator the target operator
     * @param numberOfInputs the minimum required number of input ports
     * @param numberOfOutputs the minimum required number of output ports
     * @throws IllegalStateException if the target operator is not valid
     */
    public static void checkOperatorPorts(Operator operator, int numberOfInputs, int numberOfOutputs) {
        if (operator.getInputs().size() < numberOfInputs) {
            throw new IllegalStateException(MessageFormat.format(
                    "invalid input count (must be >= {1}): {0}", //$NON-NLS-1$
                    operator,
                    numberOfInputs));
        }
        if (operator.getOutputs().size() < numberOfOutputs) {
            throw new IllegalStateException(MessageFormat.format(
                    "invalid output count (must be >= {1}): {0}", //$NON-NLS-1$
                    operator,
                    numberOfOutputs));
        }
    }

}
