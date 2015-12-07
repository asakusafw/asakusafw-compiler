/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.model.testing;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo.DataSize;

/**
 * Mock operator graph builder.
 * @since 0.1.0
 * @version 0.3.0
 */
public final class MockOperators {

    private static final TypeDescription DEFAULT_DATA_TYPE = Descriptions.typeOf(String.class);

    private static final OperatorConstraint[] EMPTY_CONSTRAINTS = new OperatorConstraint[0];

    private static final Object[] EMPTY_ATTRIBUTES = new Object[0];

    private static final AnnotationDescription ANNOTATION = new AnnotationDescription(classOf(Deprecated.class));

    private final TypeDescription commonDataType;

    private final Map<String, Operator> operators = new HashMap<>();

    /**
     * Creates a new instance.
     */
    public MockOperators() {
        this(DEFAULT_DATA_TYPE);
    }

    /**
     * Creates a new instance.
     * @param commonDataType the common data type
     */
    public MockOperators(TypeDescription commonDataType) {
        this.commonDataType = commonDataType;
    }

    /**
     * Creates a new instance.
     * @param operators operators which are created on other {@link MockOperators}
     */
    public MockOperators(Collection<? extends Operator> operators) {
        this(DEFAULT_DATA_TYPE, operators);
    }

    /**
     * Creates a new instance.
     * @param commonDataType the common data type
     * @param operators operators which are created on other {@link MockOperators}
     */
    public MockOperators(TypeDescription commonDataType, Collection<? extends Operator> operators) {
        this(commonDataType);
        for (Operator operator : operators) {
            String id = id0(operator);
            if (id != null) {
                this.operators.put(id, operator);
            }
        }
    }

    /**
     * Returns the common data type.
     * @return the common data type
     */
    public TypeDescription getCommonDataType() {
        return commonDataType;
    }

    /**
     * Adds {@link ExternalInput}.
     * @param id the operator ID
     * @return this
     */
    public MockOperators input(String id) {
        return input(id, EMPTY_ATTRIBUTES);
    }

    /**
     * Adds {@link ExternalInput}.
     * @param id the operator ID
     * @param dataSize the data size
     * @return this
     */
    public MockOperators input(String id, DataSize dataSize) {
        return input(id, dataSize, EMPTY_ATTRIBUTES);
    }

    /**
     * Adds {@link ExternalInput}.
     * @param id the operator ID
     * @param attributes the extra attributes
     * @return this
     * @since 0.3.0
     */
    public MockOperators input(String id, Object... attributes) {
        return bless(id, ExternalInput.builder(id).output(ExternalInput.PORT_NAME, commonDataType), attributes);
    }

    /**
     * Adds {@link ExternalInput}.
     * @param id the operator ID
     * @param dataSize the data size
     * @param attributes the extra attributes
     * @return this
     * @since 0.3.0
     */
    public MockOperators input(String id, DataSize dataSize, Object... attributes) {
        ExternalInputInfo info = new ExternalInputInfo.Basic(
                    new ClassDescription(id),
                    id,
                    (ClassDescription) commonDataType,
                    dataSize);
        return bless(id, ExternalInput.builder(id, info)
                .output(ExternalInput.PORT_NAME, info.getDataModelClass()), attributes);
    }

    /**
     * Adds {@link ExternalOutput}.
     * @param id the operator ID
     * @return this
     */
    public MockOperators output(String id) {
        return bless(id, ExternalOutput.builder(id).input(ExternalOutput.PORT_NAME, commonDataType));
    }

    /**
     * Adds {@link ExternalOutput}.
     * @param id the operator ID
     * @param attributes the extra attributes
     * @return this
     * @since 0.3.0
     */
    public MockOperators output(String id, Object... attributes) {
        return bless(id, ExternalOutput.builder(id).input(ExternalOutput.PORT_NAME, commonDataType), attributes);
    }

    /**
     * Adds {@link Operator} with a single input and output.
     * @param id the operator ID
     * @return this
     */
    public MockOperators operator(String id) {
        return operator(id, "in", "out", EMPTY_CONSTRAINTS); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Adds {@link Operator} with a single input and output.
     * @param id the operator ID
     * @param constraints operator constraints
     * @return this
     */
    public MockOperators operator(String id, OperatorConstraint... constraints) {
        return operator(id, "in", "out", constraints); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Adds {@link Operator}.
     * @param id the operator ID
     * @param inputNames comma separated input names
     * @param outputNames comma separated output names
     * @param constraints operator constraints
     * @return this
     */
    public MockOperators operator(
            String id, String inputNames, String outputNames,
            OperatorConstraint... constraints) {
        UserOperator.Builder builder = UserOperator.builder(
                ANNOTATION,
                new MethodDescription(classOf(MockOperators.class), id),
                classOf(MockOperators.class));
        return operator(builder, id, inputNames, outputNames, constraints);
    }

    /**
     * Adds {@link Operator}.
     * @param id the operator ID
     * @param inputNames comma separated input names
     * @param outputNames comma separated output names
     * @param attributes the extra attributes
     * @return this
     * @since 0.3.0
     */
    public MockOperators operator(
            String id, String inputNames, String outputNames,
            Object... attributes) {
        UserOperator.Builder builder = UserOperator.builder(
                ANNOTATION,
                new MethodDescription(classOf(MockOperators.class), id),
                classOf(MockOperators.class));
        return operator(builder, id, inputNames, outputNames, attributes);
    }

    /**
     * Adds {@link Operator}.
     * @param builder the operator builder
     * @param id the operator ID
     * @param constraints operator constraints
     * @return this
     */
    public MockOperators operator(
            Operator.AbstractBuilder<?, ?> builder,
            String id, OperatorConstraint... constraints) {
        return operator(builder, id, "in", "out", constraints); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Adds {@link Operator}.
     * @param builder the operator builder
     * @param id the operator ID
     * @param inputNames comma separated input names
     * @param outputNames comma separated output names
     * @param constraints operator constraints
     * @return this
     */
    public MockOperators operator(
            Operator.AbstractBuilder<?, ?> builder,
            String id, String inputNames, String outputNames, OperatorConstraint... constraints) {
        for (String name : inputNames.split(",")) { //$NON-NLS-1$
            builder.input(name, commonDataType);
        }
        for (String name : outputNames.split(",")) { //$NON-NLS-1$
            builder.output(name, commonDataType);
        }
        builder.constraint(constraints);
        return bless(id, builder);
    }

    /**
     * Adds {@link Operator}.
     * @param builder the operator builder
     * @param id the operator ID
     * @param inputNames comma separated input names
     * @param outputNames comma separated output names
     * @param attributes the extra attributes
     * @return this
     * @since 0.3.0
     */
    public MockOperators operator(
            Operator.AbstractBuilder<?, ?> builder,
            String id, String inputNames, String outputNames, Object... attributes) {
        for (String name : inputNames.split(",")) { //$NON-NLS-1$
            builder.input(name, commonDataType);
        }
        for (String name : outputNames.split(",")) { //$NON-NLS-1$
            builder.output(name, commonDataType);
        }
        return bless(id, builder, attributes);
    }

    /**
     * Adds a {@link FlowOperator}.
     * @param id the operator ID
     * @param subGraph internal graph
     * @return this
     */
    public MockOperators flow(String id, OperatorGraph subGraph) {
        return flow(id, subGraph, EMPTY_ATTRIBUTES);
    }

    /**
     * Adds a {@link FlowOperator}.
     * @param id the operator ID
     * @param subGraph internal graph
     * @param attributes the extra attributes
     * @return this
     * @since 0.3.0
     */
    public MockOperators flow(String id, OperatorGraph subGraph, Object... attributes) {
        FlowOperator.Builder builder = FlowOperator.builder(new ClassDescription(id), subGraph);
        for (ExternalInput port : subGraph.getInputs().values()) {
            builder.input(port.getName(), port.getDataType());
        }
        for (ExternalOutput port : subGraph.getOutputs().values()) {
            builder.output(port.getName(), port.getDataType());
        }
        return bless(id, builder, attributes);
    }

    /**
     * Adds {@link MarkerOperator}.
     * @param id the operator ID
     * @return this
     */
    public MockOperators marker(String id) {
        return bless(id, MarkerOperator.builder(commonDataType));
    }

    /**
     * Adds {@link MarkerOperator}.
     * @param id the operator ID
     * @param attributes the extra attributes
     * @return this
     * @since 0.3.0
     */
    public MockOperators marker(String id, Object... attributes) {
        return bless(id, MarkerOperator.builder(commonDataType), attributes);
    }

    /**
     * Adds {@link MarkerOperator}.
     * @param id the operator ID
     * @param constant the enum constant attribute
     * @param <T> the attribute type
     * @return this
     */
    public <T extends Enum<T>> MockOperators marker(String id, T constant) {
        return bless(id, MarkerOperator.builder(commonDataType).attribute(constant.getDeclaringClass(), constant));
    }

    /**
     * Adds {@link Operator}.
     * @param id the operator ID
     * @param builder the operator builder
     * @return this
     */
    public MockOperators bless(String id, Operator.BuilderBase<?, ?> builder) {
        return bless(id, builder, EMPTY_ATTRIBUTES);
    }

    /**
     * Adds {@link Operator}.
     * @param id the operator ID
     * @param builder the operator builder
     * @param attributes the extra attributes
     * @return this
     * @since 0.3.0
     */
    public MockOperators bless(String id, Operator.BuilderBase<?, ?> builder, Object... attributes) {
        for (Object attribute : attributes) {
            Class<?> type;
            if (attribute instanceof Enum<?>) {
                type = ((Enum<?>) attribute).getDeclaringClass();
            } else {
                type = attribute.getClass();
            }
            put(builder, type, attribute);
        }
        builder.attribute(Id.class, new Id(id));
        return bless(id, builder.build());
    }

    private <T> void put(Operator.BuilderBase<?, ?> builder, Class<T> type, Object attribute) {
        builder.attribute(type, type.cast(attribute));
    }

    /**
     * Registers an operator.
     * @param id the operator ID
     * @param operator the target operator
     * @return this
     */
    public MockOperators bless(String id, Operator operator) {
        operators.put(id, operator);
        return this;
    }

    /**
     * Connects between {@code <operator-id>.<port-name>} pairs.
     * @param upstream upstream port description
     * @param downstream downstream port description
     * @return this
     */
    public MockOperators connect(String upstream, String downstream) {
        OperatorOutput from = upstream(upstream);
        OperatorInput to = downstream(downstream);
        to.connect(from);
        return this;
    }

    /**
     * Validates if the operator exists.
     * @param id the operator id
     * @param kind the operator kind
     * @return this
     */
    public MockOperators assertOperator(String id, OperatorKind kind) {
        Operator operator = get(id);
        assertThat(operator.toString(), operator.getOperatorKind(), is(kind));
        return this;
    }

    /**
     * Validates if both are connected.
     * @param upstream upstream port description
     * @param downstream downstream port description
     * @return this
     */
    public MockOperators assertConnected(String upstream, String downstream) {
        return assertConnected(upstream, downstream, true);
    }

    /**
     * Validates if both are connected.
     * @param upstream upstream port description
     * @param downstream downstream port description
     * @param connected whether they are connected or not
     * @return this
     */
    public MockOperators assertConnected(String upstream, String downstream, boolean connected) {
        OperatorOutput from = upstream(upstream);
        OperatorInput to = downstream(downstream);
        assertThat(String.format("%s->%s", upstream, downstream), to.isConnected(from), is(connected)); //$NON-NLS-1$
        return this;
    }

    private OperatorOutput upstream(String expression) {
        String[] pair = pair(expression);
        Operator operator = get(pair[0]);
        OperatorOutput port;
        if (pair[1].equals("*")) { //$NON-NLS-1$
            assertThat(operator.getOutputs(), hasSize(1));
            port = operator.getOutputs().get(0);
        } else {
            port = operator.findOutput(pair[1]);
        }
        assertThat(expression, port, is(notNullValue()));
        return port;
    }

    private OperatorInput downstream(String expression) {
        String[] pair = pair(expression);
        Operator operator = get(pair[0]);
        OperatorInput port;
        if (pair[1].equals("*")) { //$NON-NLS-1$
            assertThat(operator.getInputs(), hasSize(1));
            port = operator.getInputs().get(0);
        } else {
            port = operator.findInput(pair[1]);
        }
        assertThat(expression, port, is(notNullValue()));
        return port;
    }

    private String[] pair(String pair) {
        int index = pair.indexOf('.');
        if (index < 0) {
            return new String[] { pair, "*" }; //$NON-NLS-1$
        }
        assertThat(index, is(greaterThan(0)));
        return new String[] { pair.substring(0, index), pair.substring(index + 1) };
    }

    /**
     * Returns operator.
     * @param id the operator id
     * @return the operator
     */
    public Operator get(String id) {
        assertThat(operators, hasKey(id));
        return operators.get(id);
    }

    /**
     * Returns input port.
     * @param expression port description
     * @return the port
     */
    public OperatorInput getInput(String expression) {
        return downstream(expression);
    }

    /**
     * Returns output port.
     * @param expression port description
     * @return the port
     */
    public OperatorOutput getOutput(String expression) {
        return upstream(expression);
    }

    /**
     * Returns all operators.
     * @return the operators
     */
    public Set<Operator> all() {
        return new HashSet<>(operators.values());
    }

    /**
     * Returns the id of the operator.
     * @param operator target operator (may not registered into this)
     * @return the ID
     */
    public String id(Operator operator) {
        String id = id0(operator);
        assertThat(operator.toString(), id, is(notNullValue()));
        return id;
    }

    /**
     * Returns set of operators.
     * @param ids operator IDs
     * @return operator set
     */
    public Set<Operator> getAsSet(String... ids) {
        Set<Operator> results = new LinkedHashSet<>();
        for (String id : ids) {
            results.add(get(id));
        }
        return results;
    }

    /**
     * Returns set of marker operators.
     * @param ids operator IDs
     * @return marker operator set
     */
    public Set<MarkerOperator> getMarkers(String... ids) {
        Set<MarkerOperator> results = new HashSet<>();
        for (String id : ids) {
            Operator operator = get(id);
            assertThat(operator, is(instanceOf(MarkerOperator.class)));
            results.add((MarkerOperator) operator);
        }
        return results;
    }

    /**
     * Returns the ID of the target operator.
     * @param operator the target operator
     * @return the related ID, or {@code null} if it is not defined
     */
    public static String getId(Operator operator) {
        return id0(operator);
    }

    private static String id0(Operator operator) {
        Id id = operator.getAttribute(Id.class);
        if (id == null) {
            return null;
        } else {
            return id.token;
        }
    }

    /**
     * Creates {@link OperatorGraph}.
     * @return the operator graph
     */
    public OperatorGraph toGraph() {
        return new OperatorGraph(operators.values());
    }

    private static class Id {

        final String token;

        public Id(String token) {
            this.token = token;
        }

        @Override
        public String toString() {
            return token;
        }
    }
}
