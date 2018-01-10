/**
 * Copyright 2011-2018 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.lang.compiler.analyzer;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.ExternalPort;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorArgument;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.model.graph.UserOperator;

/**
 * Inspects operator graphs.
 */
public class OperatorGraphInspector {

    private final OperatorGraphInspector parent;

    private final Set<Operator> operators;

    private final Map<String, Operator> identified = new HashMap<>();

    private final Map<String, OperatorGraphInspector> inners = new HashMap<>();

    /**
     * Creates a new instance.
     * @param graph the target operator graph
     */
    public OperatorGraphInspector(OperatorGraph graph) {
        this(null, graph);
    }

    private OperatorGraphInspector(OperatorGraphInspector parent, OperatorGraph graph) {
        this.parent = parent;
        this.operators = new HashSet<>(graph.getOperators());
    }

    /**
     * Identify an operator.
     * @param id the target ID
     * @param operator the target operator
     * @return this
     */
    public OperatorGraphInspector identify(String id, Operator operator) {
        identified.put(id, operator);
        if (operator.getOperatorKind() == OperatorKind.FLOW) {
            inners.put(id, new OperatorGraphInspector(this, ((FlowOperator) operator).getOperatorGraph()));
        }
        return this;
    }

    /**
     * Identify an operator.
     * @param id the target ID
     * @param predicate the operator predicate
     * @return this
     */
    public OperatorGraphInspector identify(String id, Predicate<Operator> predicate) {
        assertThat(identified, not(hasKey(id)));
        Operator operator = single(predicate);
        return identify(id, operator);
    }

    /**
     * Identify an input operator.
     * @param id the target ID
     * @param name the port name
     * @return this
     */
    public OperatorGraphInspector input(String id, String name) {
        return identify(id, argument -> {
            if (argument.getOperatorKind() != OperatorKind.INPUT) {
                return false;
            }
            return ((ExternalPort) argument).getName().equals(name);
        });
    }

    /**
     * Identify an output operator.
     * @param id the target ID
     * @param name the port name
     * @return this
     */
    public OperatorGraphInspector output(String id, String name) {
        return identify(id, argument -> {
            if (argument.getOperatorKind() != OperatorKind.OUTPUT) {
                return false;
            }
            return ((ExternalPort) argument).getName().equals(name);
        });
    }

    /**
     * Identify a flow operator.
     * @param id the target ID
     * @param description the description
     * @return this
     */
    public OperatorGraphInspector flowpart(String id, Class<?> description) {
        return identify(id, argument -> {
            if (argument.getOperatorKind() != OperatorKind.FLOW) {
                return false;
            }
            ClassDescription desc = ((FlowOperator) argument).getDescriptionClass();
            if (desc.getBinaryName().equals(description.getName()) == false) {
                return false;
            }
            return true;
        });
    }

    /**
     * Identify an operator.
     * @param id the target ID
     * @param kind the core operator kind
     * @return this
     */
    public OperatorGraphInspector operator(String id, CoreOperatorKind kind) {
        return identify(id, argument -> {
            if (argument.getOperatorKind() != OperatorKind.CORE) {
                return false;
            }
            return ((CoreOperator) argument).getCoreOperatorKind() == kind;
        });
    }

    /**
     * Identify an operator.
     * @param id the target ID
     * @param operatorId the operator ID specified in {@link MockOperator#id()}
     * @return this
     */
    public OperatorGraphInspector operator(String id, String operatorId) {
        return identify(id, argument -> {
            if (argument.getOperatorKind() != OperatorKind.USER) {
                return false;
            }
            MethodDescription method = ((UserOperator) argument).getMethod();
            try {
                Method resolved = method.resolve(getClass().getClassLoader());
                MockOperator annotation = resolved.getAnnotation(MockOperator.class);
                if (annotation == null) {
                    return false;
                }
                if (annotation.id().equals(operatorId) == false) {
                    return false;
                }
                return true;
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        });
    }

    /**
     * Identify an operator.
     * @param id the target ID
     * @param operatorClass the operator class
     * @param methodName the operator method name
     * @return this
     */
    public OperatorGraphInspector operator(String id, Class<?> operatorClass, String methodName) {
        return identify(id, argument -> {
            if (argument.getOperatorKind() != OperatorKind.USER) {
                return false;
            }
            MethodDescription method = ((UserOperator) argument).getMethod();
            if (method.getDeclaringClass().getBinaryName().equals(operatorClass.getName()) == false) {
                return false;
            }
            if (method.getName().equals(methodName) == false) {
                return false;
            }
            return true;
        });
    }

    /**
     * Returns a nested inspector.
     * @param id the flow operator ID
     * @return the nested inspector
     */
    public OperatorGraphInspector enter(String id) {
        assertThat(inners, hasKey(id));
        return inners.get(id);
    }

    /**
     * Returns the outer inspector
     * @return the outer inspector
     */
    public OperatorGraphInspector exit() {
        assertThat(parent, is(notNullValue()));
        return parent;
    }

    /**
     * Returns an operator.
     * @param id the operator ID
     * @return the found
     */
    public Operator get(String id) {
        assertThat(identified, hasKey(id));
        return identified.get(id);
    }

    /**
     * Returns an input port.
     * @param ref the port expression
     * @return the input port
     */
    public OperatorInput getInput(String ref) {
        String[] pair = pair(ref);
        String id = pair[0];
        Operator element = get(id);
        return getPort(ref, element.getInputs());
    }

    /**
     * Returns an output port.
     * @param ref the port expression
     * @return the output port
     */
    public OperatorOutput getOutput(String ref) {
        String[] pair = pair(ref);
        String id = pair[0];
        Operator element = get(id);
        return getPort(ref, element.getOutputs());
    }

    /**
     * Returns an argument.
     * @param ref the port expression
     * @return the argument
     */
    public OperatorArgument getArgument(String ref) {
        String[] pair = pair(ref);
        String id = pair[0];
        Operator element = get(id);
        String name = pair[1];
        OperatorArgument argument = element.findArgument(name);
        assertThat(ref, argument, is(notNullValue()));
        return argument;
    }

    /**
     * Asserts whether each port is connected or not.
     * @param upstreamRef upstream port expression
     * @param downstreamRef downstream port expression
     * @return this
     */
    public OperatorGraphInspector connected(String upstreamRef, String downstreamRef) {
        OperatorOutput upstream = getOutput(upstreamRef);
        OperatorInput downstream = getInput(downstreamRef);
        assertThat(String.format("%s->%s", upstreamRef, downstreamRef), upstream.isConnected(downstream), is(true));
        return this;
    }

    /**
     * Asserts total number of operators.
     * @param count the expected operator count
     * @return this
     */
    public OperatorGraphInspector operators(int count) {
        assertThat(operators, hasSize(count));
        return this;
    }

    /**
     * Asserts total number of connections.
     * @param count the expected connections count
     * @return this
     */
    public OperatorGraphInspector connections(int count) {
        int total = 0;
        for (Operator operator : operators) {
            for (OperatorInput input : operator.getInputs()) {
                total += input.getOpposites().size();
            }
        }
        assertThat(total, is(count));
        return this;
    }

    private <T extends OperatorPort> T getPort(String ref, List<T> ports) {
        String[] pair = pair(ref);
        String id = pair[0];
        String name = pair[1];
        if (name.equals("*")) {
            assertThat(id, ports, hasSize(1));
            return ports.get(0);
        }
        for (T port : ports) {
            if (port.getName().equals(name)) {
                return port;
            }
        }
        try {
            int index = Integer.parseInt(name);
            if (index >= 0 && index < ports.size()) {
                return ports.get(index);
            }
        } catch (NumberFormatException e) {
            // continue
        }
        throw new AssertionError(MessageFormat.format(
                "missing port: {0}",
                ref));
    }

    private static String[] pair(String ref) {
        int index = ref.indexOf('.');
        if (index < 0) {
            return new String[] { ref, "*" };
        } else {
            return new String[] { ref.substring(0, index), ref.substring(index + 1) };
        }
    }

    private Operator single(Predicate<Operator> predicate) {
        Set<Operator> candidates = findAll(predicate);
        assertThat(candidates, hasSize(1));
        return candidates.iterator().next();
    }

    private Set<Operator> findAll(Predicate<Operator> predicate) {
        Set<Operator> results = new HashSet<>(2);
        for (Operator operator : operators) {
            if (predicate.test(operator)) {
                results.add(operator);
            }
        }
        return results;
    }
}
