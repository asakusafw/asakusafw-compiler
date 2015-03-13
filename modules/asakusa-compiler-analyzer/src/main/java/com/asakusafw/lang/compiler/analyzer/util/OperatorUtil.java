package com.asakusafw.lang.compiler.analyzer.util;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;

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

    /**
     * Collects data types which directly used in the specified operators.
     * Note that, the result does not contain data types occurred in some flow operators.
     * @param operators the target operators
     * @return the data types occurred in the operators
     * @see FlowOperator
     * @see OperatorGraph#getOperators()
     */
    public static Set<TypeDescription> collectDataTypes(Collection<? extends Operator> operators) {
        Set<TypeDescription> results = new HashSet<>();
        for (Operator operator : operators) {
            collectDataTypes(results, operator);
        }
        return results;
    }

    private static void collectDataTypes(Collection<? super TypeDescription> results, Operator operator) {
        for (OperatorPort port : operator.getInputs()) {
            results.add(port.getDataType());
        }
        for (OperatorPort port : operator.getOutputs()) {
            results.add(port.getDataType());
        }
    }
}
