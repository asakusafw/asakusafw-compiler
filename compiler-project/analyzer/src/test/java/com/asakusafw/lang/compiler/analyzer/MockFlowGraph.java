/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.analyzer.util.TypeInfo;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.graph.FlowElement;
import com.asakusafw.vocabulary.flow.graph.FlowElementAttribute;
import com.asakusafw.vocabulary.flow.graph.FlowElementDescription;
import com.asakusafw.vocabulary.flow.graph.FlowElementInput;
import com.asakusafw.vocabulary.flow.graph.FlowElementKind;
import com.asakusafw.vocabulary.flow.graph.FlowElementOutput;
import com.asakusafw.vocabulary.flow.graph.FlowElementPort;
import com.asakusafw.vocabulary.flow.graph.FlowElementPortDescription;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;
import com.asakusafw.vocabulary.flow.graph.FlowIn;
import com.asakusafw.vocabulary.flow.graph.FlowOut;
import com.asakusafw.vocabulary.flow.graph.InputDescription;
import com.asakusafw.vocabulary.flow.graph.OperatorDescription;
import com.asakusafw.vocabulary.flow.graph.OutputDescription;
import com.asakusafw.vocabulary.flow.graph.PortConnection;
import com.asakusafw.vocabulary.flow.graph.PortDirection;
import com.asakusafw.vocabulary.flow.graph.ShuffleKey;
import com.asakusafw.vocabulary.flow.util.PseudElementDescription;
import com.asakusafw.vocabulary.model.Key;

/**
 * Mock {@link FlowGraph}.
 */
public class MockFlowGraph {

    private final List<FlowIn<?>> flowInputs = new ArrayList<>();

    private final List<FlowOut<?>> flowOutputs = new ArrayList<>();

    private final Map<String, FlowElement> elements = new HashMap<>();

    /**
     * Creates a {@link FlowGraph} from this.
     * @return the created graph
     */
    public FlowGraph toGraph() {
        return toGraph(FlowDescription.class);
    }

    /**
     * Creates a {@link FlowGraph} from this.
     * @param description the flow description
     * @return the created graph
     */
    public FlowGraph toGraph(Class<? extends FlowDescription> description) {
        return new FlowGraph(description, flowInputs, flowOutputs);
    }

    /**
     * Adds an element to this.
     * @param id the element ID
     * @param description the element description
     * @return this
     */
    public MockFlowGraph add(String id, FlowElementDescription description) {
        assertThat(elements, not(hasKey(id)));
        FlowElement element;
        if (description.getKind() == FlowElementKind.INPUT) {
            FlowIn<?> port = new FlowIn<>((InputDescription) description);
            flowInputs.add(port);
            element = port.getFlowElement();
        } else if (description.getKind() == FlowElementKind.OUTPUT) {
            FlowOut<?> port = new FlowOut<>((OutputDescription) description);
            flowOutputs.add(port);
            element = port.getFlowElement();
        } else {
            element = new FlowElement(description);
        }
        elements.put(id, element);
        return this;
    }

    /**
     * Adds an pseudo-element to this.
     * @param id the element ID
     * @param type the port type
     * @param attributes extra attributes
     * @return this
     */
    public MockFlowGraph pseudo(String id, Class<?> type, FlowElementAttribute... attributes) {
        return add(id, new PseudElementDescription(
                id, type,
                true, true,
                attributes));
    }

    /**
     * Adds an operator which is annotated by <code>&#64;MockOperator</code>.
     * @param id the element ID
     * @param operatorClass the operator class
     * @param methodName the method name
     * @return this
     */
    public MockFlowGraph operator(String id, Class<?> operatorClass, String methodName) {
        return operator(id, operatorClass, methodName, Collections.emptyMap());
    }

    /**
     * Adds an operator which is annotated by <code>&#64;MockOperator</code>.
     * @param id the element ID
     * @param operatorClass the operator class
     * @param methodName the method name
     * @param args method arguments for value parameters
     * @param attributes element attributes
     * @return this
     */
    public MockFlowGraph operator(
            String id,
            Class<?> operatorClass, String methodName,
            Map<String, ?> args, FlowElementAttribute... attributes) {
        Method method = getMockOperatorMethod(operatorClass, methodName);
        MockOperator annotation = method.getAnnotation(MockOperator.class);
        assertThat(annotation, is(notNullValue()));

        Type[] parameterTypes = method.getGenericParameterTypes();
        String[] parameterNames = annotation.parameters();
        if (parameterNames.length == 0) {
            parameterNames = new String[parameterTypes.length];
            for (int i = 0; i < parameterNames.length; i++) {
                parameterNames[i] = String.format("_%d", i);
            }
        }
        assertThat("parameter size", parameterNames.length, equalTo(method.getParameterTypes().length));

        List<FlowElementPortDescription> inputs = new ArrayList<>();
        List<FlowElementPortDescription> outputs = new ArrayList<>();
        List<OperatorDescription.Parameter> arguments = new ArrayList<>();
        for (int i = 0, n = parameterTypes.length; i < n; i++) {
            TypeInfo type = TypeInfo.of(parameterTypes[i]);
            String name = parameterNames[i];

            if (type.getRawType().isPrimitive()) {
                assertThat(args, hasKey(name));
                arguments.add(new OperatorDescription.Parameter(name, type.getRawType(), args.get(name)));
            } else if (type.getRawType() == Result.class) {
                outputs.add(new FlowElementPortDescription(
                        name,
                        type.getErasedTypeArguments().get(0),
                        PortDirection.OUTPUT));
            } else if (type.getRawType() == List.class) {
                ShuffleKey key = extractShuffleKey(method.getParameterAnnotations()[i]);
                inputs.add(new FlowElementPortDescription(
                        name,
                        type.getErasedTypeArguments().get(0),
                        key));
            } else {
                inputs.add(new FlowElementPortDescription(
                        name,
                        type.getRawType(),
                        PortDirection.INPUT));
            }
        }
        OperatorDescription description = new OperatorDescription(
                new OperatorDescription.Declaration(
                        MockOperator.class,
                        operatorClass, operatorClass,
                        methodName, Arrays.asList(method.getParameterTypes())),
                inputs,
                outputs,
                Collections.emptyList(),
                arguments,
                Arrays.asList(attributes));
        return add(id, description);
    }

    private ShuffleKey extractShuffleKey(Annotation[] annotations) {
        Key key = getKeyAnnotation(annotations);
        List<String> group = Arrays.asList(key.group());
        List<ShuffleKey.Order> order = new ArrayList<>();
        for (String expr : key.order()) {
            assertThat(expr, anyOf(startsWith("+"), startsWith("-")));
            order.add(new ShuffleKey.Order(
                    expr.substring(1),
                    expr.charAt(0) == '+' ? ShuffleKey.Direction.ASC : ShuffleKey.Direction.DESC));
        }
        return new ShuffleKey(group, order);
    }

    private Key getKeyAnnotation(Annotation[] annotations) {
        for (Annotation annotation : annotations) {
            if (annotation instanceof Key) {
                return (Key) annotation;
            }
        }
        throw new AssertionError();
    }

    private Method getMockOperatorMethod(Class<?> operatorClass, String methodName) {
        for (Method method : operatorClass.getMethods()) {
            if (method.getName().equals(methodName) == false) {
                continue;
            }
            if (method.isAnnotationPresent(MockOperator.class) == false) {
                continue;
            }
            return method;
        }
        throw new AssertionError(MessageFormat.format(
                "{0}#{1}",
                operatorClass.getName(),
                methodName));
    }

    /**
     * Connects each ports.
     * @param upstreamRef upstream port expression
     * @param downstreamRef downstream port expression
     * @return this
     */
    public MockFlowGraph connect(String upstreamRef, String downstreamRef) {
        FlowElementOutput upstream = getOutput(upstreamRef);
        FlowElementInput downstream = getInput(downstreamRef);
        PortConnection.connect(upstream, downstream);
        return this;
    }

    /**
     * Returns an element.
     * @param id the element ID
     * @return the element
     */
    public FlowElement get(String id) {
        assertThat(id, elements, hasKey(id));
        return elements.get(id);
    }

    /**
     * Returns an input port.
     * @param ref the port expression
     * @return the input port
     */
    public FlowElementInput getInput(String ref) {
        String[] pair = pair(ref);
        String id = pair[0];
        FlowElement element = get(id);
        return getPort(ref, element.getInputPorts());
    }

    /**
     * Returns an output port.
     * @param ref the port expression
     * @return the output port
     */
    public FlowElementOutput getOutput(String ref) {
        String[] pair = pair(ref);
        String id = pair[0];
        FlowElement element = get(id);
        return getPort(ref, element.getOutputPorts());
    }

    private <T extends FlowElementPort> T getPort(String ref, List<T> ports) {
        String[] pair = pair(ref);
        String id = pair[0];
        String name = pair[1];
        if (name.equals("*")) {
            assertThat(id, ports, hasSize(1));
            return ports.get(0);
        }
        for (T port : ports) {
            if (port.getDescription().getName().equals(name)) {
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
}
