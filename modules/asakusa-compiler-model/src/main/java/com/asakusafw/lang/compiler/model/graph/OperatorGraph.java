package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents DAG of operators.
 */
public class OperatorGraph {

    private final Set<Operator> operators = new HashSet<>();

    /**
     * Creates a new instance.
     */
    public OperatorGraph() {
        return;
    }

    /**
     * Creates a new instance.
     * @param operators the member operators
     */
    public OperatorGraph(Collection<? extends Operator> operators) {
        this.operators.addAll(operators);
    }

    /**
     * Adds an operator to this graph.
     * @param operator the target operator
     * @return this
     * @see #rebuild()
     */
    public OperatorGraph add(Operator operator) {
        operators.add(operator);
        return this;
    }

    /**
     * Removes the operator from this graph.
     * If the operator is reachable from other operators in this graph,
     * the {@link #rebuild()} may restore the removed operator.
     * @see Operator#disconnectAll()
     * @param operator the target operator
     * @return this
     */
    public OperatorGraph remove(Operator operator) {
        operators.remove(operator);
        return this;
    }

    /**
     * Removes all operators in this graph.
     * @return this
     */
    public OperatorGraph clear() {
        operators.clear();
        return this;
    }

    /**
     * Returns operators in this graph.
     * If connections between operators are changed, clients may
     * {@link #rebuild() rebuild this graph} as well before invokes this method.
     * @return this
     * @see #rebuild()
     * @see #getAllOperators(Collection)
     */
    public Collection<Operator> getOperators() {
        return new ArrayList<>(operators);
    }

    /**
     * Returns {@link ExternalInput} map (which key is its name) in this graph.
     * If connections between operators are changed, clients may
     * {@link #rebuild() rebuild this graph} as well before invokes this method.
     * @return this
     * @see #rebuild()
     */
    public Map<String, ExternalInput> getInputs() {
        return getPorts(ExternalInput.class);
    }

    /**
     * Returns {@link ExternalOutput} map (which key is its name) in this graph.
     * If connections between operators are changed, clients may
     * {@link #rebuild() rebuild this graph} as well before invokes this method.
     * @return this
     * @see #rebuild()
     */
    public Map<String, ExternalOutput> getOutputs() {
        return getPorts(ExternalOutput.class);
    }

    private <T extends ExternalPort> Map<String, T> getPorts(Class<T> type) {
        Map<String, T> results = new HashMap<>();
        for (Operator operator : operators) {
            if (type.isInstance(operator)) {
                T port = type.cast(operator);
                results.put(port.getName(), port);
            }
        }
        return results;
    }

    /**
     * Collects reachable operators from members in this graph, and adds them to this.
     * @return this
     * @see #getAllOperators(Collection)
     */
    public OperatorGraph rebuild() {
        Set<Operator> newOperators = getAllOperators(operators);
        this.operators.clear();
        this.operators.addAll(newOperators);
        return this;
    }

    /**
     * Returns the copy of this graph.
     * Each operator in this graph is {@link Operator#copy() copied} recursively (includes all reachable operators),
     * and connected each other.
     * @return the copy of this graph
     * @see #copy(Collection)
     */
    public OperatorGraph copy() {
        Map<Operator, Operator> map = copy(getAllOperators(operators));
        Collection<Operator> results = new ArrayList<>();
        for (Operator operator : operators) {
            Operator mapped = map.get(operator);
            assert mapped != null;
            results.add(mapped);
        }
        return new OperatorGraph(results);
    }

    /**
     * Returns all operators which are reachable from the specified operators.
     * @param operators the operators
     * @return the all operators
     */
    public static Set<Operator> getAllOperators(Collection<? extends Operator> operators) {
        Set<Operator> saw = new HashSet<>(operators);
        LinkedList<Operator> work = new LinkedList<>(operators);
        while (work.isEmpty() == false) {
            Operator operator = work.removeFirst();
            for (OperatorPort port : operator.getInputs()) {
                for (OperatorPort opposite : port.getOpposites()) {
                    Operator owner = opposite.getOwner();
                    if (saw.contains(owner) == false) {
                        work.add(owner);
                        saw.add(owner);
                    }
                }
            }
            for (OperatorPort port : operator.getOutputs()) {
                for (OperatorPort opposite : port.getOpposites()) {
                    Operator owner = opposite.getOwner();
                    if (saw.contains(owner) == false) {
                        work.add(operator);
                        saw.add(owner);
                    }
                }
            }
        }
        return saw;
    }

    /**
     * Returns a copy of operators.
     * Each operator is {@link Operator#copy() copied} recursively,
     * and it connected each other only if the neighbor operators are also copied in this invocation.
     * @param operators the target operators (maps an original operator into its copy.
     * @return a map of original operator to the corresponded copy
     */
    public static Map<Operator, Operator> copy(Collection<? extends Operator> operators) {
        Map<Operator, Operator> map = new HashMap<>();
        Map<OperatorInput, OperatorInput> inputs = new HashMap<>();
        Map<OperatorOutput, OperatorOutput> outputs = new HashMap<>();
        for (Operator operator : operators) {
            Operator copy = operator.copy();
            map.put(operator, copy);
            List<? extends OperatorProperty> from = operator.getProperties();
            List<? extends OperatorProperty> to = copy.getProperties();
            assert from.size() == to.size();
            for (int i = 0, n = from.size(); i < n; i++) {
                OperatorProperty source = from.get(i);
                OperatorProperty target = to.get(i);
                assert source.getPropertyKind() == target.getPropertyKind();
                switch (source.getPropertyKind()) {
                case INPUT:
                    inputs.put((OperatorInput) source, (OperatorInput) target);
                    break;
                case OUTPUT:
                    outputs.put((OperatorOutput) source, (OperatorOutput) target);
                    break;
                default:
                    break;
                }
            }
        }
        for (Map.Entry<OperatorInput, OperatorInput> entry : inputs.entrySet()) {
            OperatorInput from = entry.getKey();
            OperatorInput to = entry.getValue();
            assert to.getOpposites().isEmpty();
            for (OperatorOutput upstream : from.getOpposites()) {
                if (outputs.containsKey(upstream)) {
                    OperatorOutput mapped = outputs.get(upstream);
                    to.connect(mapped);
                }
            }
        }
        return map;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "OperatorGraph({0}operators)", //$NON-NLS-1$
                operators.size());
    }
}
