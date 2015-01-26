package com.asakusafw.lang.compiler.model.graph;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Utilities for {@link Operator}.
 */
public final class Operators {

    private Operators() {
        return;
    }

    /**
     * Returns the successors of the operator.
     * @param operator the operator
     * @return the successors
     */
    public static Set<Operator> getSuccessors(Operator operator) {
        return getSuccessors(operator.getOutputs());
    }

    /**
     * Returns the predecessors of the operator.
     * @param operator the operator
     * @return the predecessors
     */
    public static Set<Operator> getPredecessors(Operator operator) {
        return getPredecessors(operator.getInputs());
    }

    /**
     * Returns the successors of the operator outputs.
     * @param ports the output ports
     * @return the successors
     */
    public static Set<Operator> getSuccessors(Collection<OperatorOutput> ports) {
        return getOpposites(ports);
    }

    /**
     * Returns the predecessors of the operator inputs.
     * @param ports the input ports
     * @return the predecessors
     */
    public static Set<Operator> getPredecessors(Collection<OperatorInput> ports) {
        return getOpposites(ports);
    }

    /**
     * Returns the nearest reachable successors which satisfy the predicate
     * from the target operator outputs.
     * @param ports the output ports
     * @param predicate the predicate of target operators
     * @return the found operators
     */
    public static Set<Operator> findNearestReachableSuccessors(
            Collection<OperatorOutput> ports,
            Predicate<? super Operator> predicate) {
        Set<Operator> results = new HashSet<>();
        Set<Operator> saw = new HashSet<>();
        LinkedList<Operator> work = new LinkedList<>(getSuccessors(ports));
        while (work.isEmpty() == false) {
            Operator operator = work.removeFirst();
            if (saw.contains(operator)) {
                continue;
            }
            saw.add(operator);
            if (predicate.apply(operator)) {
                results.add(operator);
            } else {
                collectOpposites(operator.getOutputs(), work);
            }
        }
        return results;
    }

    /**
     * Returns the nearest reachable predecessors which satisfy the predicate
     * from the target operator inputs.
     * @param ports the input ports
     * @param predicate the predicate of target operators
     * @return the found operators
     */
    public static Set<Operator> findNearestReachablePredecessors(
            Collection<OperatorInput> ports,
            Predicate<? super Operator> predicate) {
        Set<Operator> results = new HashSet<>();
        Set<Operator> saw = new HashSet<>();
        LinkedList<Operator> work = new LinkedList<>(getPredecessors(ports));
        while (work.isEmpty() == false) {
            Operator operator = work.removeFirst();
            if (saw.contains(operator)) {
                continue;
            }
            saw.add(operator);
            if (predicate.apply(operator)) {
                results.add(operator);
            } else {
                collectOpposites(operator.getInputs(), work);
            }
        }
        return results;
    }

    /**
     * Collects and returns the operators until nearest reachable successors are detected
     * from the target operator outputs.
     * @param ports the output ports
     * @param predicate the predicate of target operators
     * @param inclusive {@code true} if the result includes the nearest reachable successors
     * @return the found operators
     */
    public static Set<Operator> collectUntilNearestReachableSuccessors(
            Collection<OperatorOutput> ports,
            Predicate<? super Operator> predicate,
            boolean inclusive) {
        Set<Operator> results = new HashSet<>();
        Set<Operator> saw = new HashSet<>();
        LinkedList<Operator> work = new LinkedList<>(getSuccessors(ports));
        while (work.isEmpty() == false) {
            Operator operator = work.removeFirst();
            if (saw.contains(operator)) {
                continue;
            }
            if (predicate.apply(operator)) {
                if (inclusive) {
                    results.add(operator);
                }
            } else {
                results.add(operator);
                collectOpposites(operator.getOutputs(), work);
            }
        }
        return results;
    }

    /**
     * Collects and returns the operators until nearest reachable predecessors are detected
     * from the target operator inputs.
     * @param ports the input ports
     * @param predicate the predicate of target operators
     * @param inclusive {@code true} if the result includes the nearest reachable predecessors
     * @return the found operators
     */
    public static Set<Operator> collectUntilNearestReachablePredecessors(
            Collection<OperatorInput> ports,
            Predicate<? super Operator> predicate,
            boolean inclusive) {
        Set<Operator> results = new HashSet<>();
        Set<Operator> saw = new HashSet<>();
        LinkedList<Operator> work = new LinkedList<>(getPredecessors(ports));
        while (work.isEmpty() == false) {
            Operator operator = work.removeFirst();
            if (saw.contains(operator)) {
                continue;
            }
            saw.add(operator);
            if (predicate.apply(operator)) {
                if (inclusive) {
                    results.add(operator);
                }
            } else {
                results.add(operator);
                collectOpposites(operator.getInputs(), work);
            }
        }
        return results;
    }

    private static Set<Operator> getOpposites(Collection<? extends OperatorPort> ports) {
        return collectOpposites(ports, new HashSet<Operator>());
    }

    private static <T extends Collection<? super Operator>> T collectOpposites(
            Collection<? extends OperatorPort> ports, T sink) {
        for (OperatorPort port : ports) {
            for (OperatorPort opposite : port.getOpposites()) {
                sink.add(opposite.getOwner());
            }
        }
        return sink;
    }

    /**
     * Connects between all output and inputs.
     * @param upstream upstream port
     * @param downstreams downstream ports
     */
    public static void connectAll(OperatorOutput upstream, Collection<OperatorInput> downstreams) {
        for (OperatorInput downstream : downstreams) {
            upstream.connect(downstream);
        }
    }

    /**
     * Connects between all outputs and input.
     * @param upstreams upstream ports
     * @param downstream downstream port
     */
    public static void connectAll(Collection<OperatorOutput> upstreams, OperatorInput downstream) {
        for (OperatorOutput upstream : upstreams) {
            upstream.connect(downstream);
        }
    }

    /**
     * Connects between all outputs and inputs.
     * @param upstreams upstream ports
     * @param downstreams downstream ports
     */
    public static void connectAll(Collection<OperatorOutput> upstreams, Collection<OperatorInput> downstreams) {
        for (OperatorOutput upstream : upstreams) {
            connectAll(upstream, downstreams);
        }
    }

    /**
     * Inserts a &quot;straight&quot; operator into the target port.
     * The strait operator must only have a single input and output port.
     * By the result, the specified port will be only connected to the target operator.
     * @param operator the target operator
     * @param port the target port
     * @param <T> the target operator type
     * @return the target operator
     */
    public static <T extends Operator> T insert(T operator, OperatorOutput port) {
        List<OperatorInput> inputs = operator.getInputs();
        List<OperatorOutput> outputs = operator.getOutputs();
        checkStraight(inputs, outputs);
        connectAll(outputs.get(0), port.getOpposites());
        port.disconnectAll();
        port.connect(inputs.get(0));
        return operator;
    }

    /**
     * Inserts a &quot;straight&quot; operator into the target port.
     * The strait operator must only have a single input and output port.
     * By the result, the specified port will be only connected to the target operator.
     * @param operator the target operator
     * @param port the target port
     * @param <T> the target operator type
     * @return the target operator
     */
    public static <T extends Operator> T insert(T operator, OperatorInput port) {
        List<OperatorInput> inputs = operator.getInputs();
        List<OperatorOutput> outputs = operator.getOutputs();
        checkStraight(inputs, outputs);
        connectAll(port.getOpposites(), inputs.get(0));
        port.disconnectAll();
        port.connect(outputs.get(0));
        return operator;
    }

    /**
     * Inserts a &quot;straight&quot; operator into the target connection.
     * The strait operator must only have a single input and output port.
     * @param operator the target operator
     * @param upstream the upstream port
     * @param downstream the downstream
     * @param <T> the target operator type
     * @return the target operator
     */
    public static <T extends Operator> T insert(T operator, OperatorOutput upstream, OperatorInput downstream) {
        if (upstream.isConnected(downstream) == false) {
            throw new IllegalArgumentException();
        }
        List<OperatorInput> inputs = operator.getInputs();
        List<OperatorOutput> outputs = operator.getOutputs();
        checkStraight(inputs, outputs);
        upstream.disconnect(downstream);
        upstream.connect(inputs.get(0));
        downstream.connect(outputs.get(0));
        return operator;
    }

    /**
     * Removes a &quot;straight&quot; operator and bypass the original previous successors and predecessors.
     * The strait operator must only have a single input and output port.
     * The target operator will {@link Operator#disconnectAll() have no successors nor predecessors}.
     * @param operator the target operator
     * @param <T> the target operator type
     * @return the target operator
     */
    public static <T extends Operator> T remove(T operator) {
        List<OperatorInput> inputs = operator.getInputs();
        List<OperatorOutput> outputs = operator.getOutputs();
        checkStraight(inputs, outputs);
        connectAll(inputs.get(0).getOpposites(), outputs.get(0).getOpposites());
        operator.disconnectAll();
        return operator;
    }

    /**
     * Replaces a &quot;straight&quot; operator with another &quot;straight&quot; operator.
     * The strait operator must only have a single input and output port.
     * The original operator will {@link Operator#disconnectAll() have no successors nor predecessors}.
     * @param oldOperator the operator to be replaced
     * @param newOperator the replacement operator
     */
    public static void replace(Operator oldOperator, Operator newOperator) {
        List<OperatorInput> oldInputs = oldOperator.getInputs();
        List<OperatorOutput> oldOutputs = oldOperator.getOutputs();
        checkStraight(oldInputs, oldOutputs);
        List<OperatorInput> newInputs = newOperator.getInputs();
        List<OperatorOutput> newOutputs = newOperator.getOutputs();
        checkStraight(newInputs, newOutputs);
        connectAll(oldInputs.get(0).getOpposites(), newInputs.get(0));
        connectAll(newOutputs.get(0), oldOutputs.get(0).getOpposites());
        oldOperator.disconnectAll();
    }

    private static void checkStraight(List<OperatorInput> inputs, List<OperatorOutput> outputs) {
        if (inputs.size() != 1) {
            throw new IllegalArgumentException();
        }
        if (outputs.size() != 1) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Represents a predicate of finding operators.
     * @param <T> the target type
     */
    public interface Predicate<T> {

        /**
         * Returns whether the argument satisfies this predicate or not.
         * @param argument the target argument
         * @return {@code true} if the argument satisfies this, otherwise {@code false}
         */
        boolean apply(T argument);
    }
}
