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
package com.asakusafw.lang.compiler.tester.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.asakusafw.lang.compiler.analyzer.ExternalPortAnalyzer;
import com.asakusafw.lang.compiler.core.CompilerContext;
import com.asakusafw.lang.compiler.core.adapter.ExternalPortAnalyzerAdapter;
import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Builds {@link OperatorGraph}.
 * @since 0.4.0
 */
public class OperatorGraphBuilder {

    private final CompilerContext context;

    private final Map<String, Operator> operators = new LinkedHashMap<>();

    /**
     * Creates a new instance.
     * @param context the current context
     */
    public OperatorGraphBuilder(CompilerContext context) {
        Arguments.requireNonNull(context);
        this.context = context;
    }

    /**
     * Returns the context.
     * @return the context
     */
    public CompilerContext getContext() {
        return context;
    }

    /**
     * Builds {@link OperatorGraph}.
     * @return the built graph
     */
    public OperatorGraph build() {
        return new OperatorGraph(operators.values());
    }

    /**
     * Adds an operator to this.
     * @param id the operator ID
     * @param operator the target operator
     * @return this
     */
    public OperatorGraphBuilder add(String id, Operator operator) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(operator);
        Invariants.require(operators.containsKey(id) == false);
        operators.merge(id, operator, (a, b) -> {
            throw new IllegalStateException(id);
        });
        return this;
    }

    /**
     * Adds an external input.
     * @param id the operator ID
     * @param description the description
     * @return this
     */
    public OperatorGraphBuilder input(String id, ImporterDescription description) {
        ExternalPortAnalyzer analyzer = new ExternalPortAnalyzerAdapter(context);
        return input(id, analyzer.analyze(id, description));
    }

    /**
     * Adds an external output.
     * @param id the operator ID
     * @param description the description
     * @return this
     */
    public OperatorGraphBuilder output(String id, ExporterDescription description) {
        ExternalPortAnalyzer analyzer = new ExternalPortAnalyzerAdapter(context);
        return output(id, analyzer.analyze(id, description));
    }

    /**
     * Adds an external input.
     * @param id the operator ID
     * @param info the external port info
     * @return this
     */
    public OperatorGraphBuilder input(String id, ExternalInputInfo info) {
        ExternalInput operator = ExternalInput.newInstance(id, info);
        return add(id, operator);
    }

    /**
     * Adds an external output.
     * @param id the operator ID
     * @param info the external port info
     * @return this
     */
    public OperatorGraphBuilder output(String id, ExternalOutputInfo info) {
        ExternalOutput operator = ExternalOutput.newInstance(id, info);
        return add(id, operator);
    }

    /**
     * Adds a core operator.
     * @param id the operator ID
     * @param operatorKind the core operator kind
     * @param configurator the operator configurator
     * @return this
     */
    public OperatorGraphBuilder operator(
            String id,
            CoreOperatorKind operatorKind,
            Function<CoreOperator.Builder, Operator> configurator) {
        return add(id, configurator.apply(CoreOperator.builder(operatorKind)));
    }

    /**
     * Adds a user operator.
     * @param id the operator ID
     * @param operatorClass the operator class
     * @param methodName the operator method name
     * @param configurator the operator configurator
     * @return this
     */
    public OperatorGraphBuilder operator(
            String id,
            Class<?> operatorClass, String methodName,
            Function<UserOperator.Builder, Operator> configurator) {
        Method method = getOperatorMethod(operatorClass, methodName);
        AnnotationDescription annotation = getOperatorAnnotation(method);
        UserOperator.Builder builder = UserOperator.builder(
                annotation,
                MethodDescription.of(method),
                ClassDescription.of(operatorClass));
        return add(id, configurator.apply(builder));
    }

    private Method getOperatorMethod(Class<?> operatorClass, String name) {
        return Stream.of(operatorClass.getMethods())
                .filter(m -> m.getName().equals(name))
                .findAny()
                .orElseThrow(() -> new IllegalStateException(MessageFormat.format(
                        "cannot detect for operator method: {0}#{1}",
                        operatorClass.getName(), name)));
    }

    private AnnotationDescription getOperatorAnnotation(Method method) {
        Annotation[] annotations = method.getAnnotations();
        if (annotations.length == 1) {
            return AnnotationDescription.of(annotations[0]);
        }
        List<Annotation> candidates = Stream.of(annotations)
                .filter(a -> a.annotationType().getName().startsWith("com.asakusafw.vocabulary."))
                .collect(Collectors.toList());
        if (candidates.size() == 1) {
            return AnnotationDescription.of(candidates.get(0));
        }
        throw new IllegalStateException(MessageFormat.format(
                "cannot detect for operator annotation: {0}",
                method));
    }

    /**
     * Connects upstream into each downstream.
     * @param upstream the upstream port notation
     * @param downstreams the downstream port notations
     * @return this
     */
    public OperatorGraphBuilder connect(String upstream, String... downstreams) {
        OperatorOutput source = getOutput(upstream);
        for (String downstream : downstreams) {
            OperatorInput destination = getInput(downstream);
            source.connect(destination);
        }
        return this;
    }

    /**
     * Returns the output port.
     * @param notation the output port notation
     * @return the output port
     */
    public OperatorOutput output(String notation) {
        return getOutput(notation);
    }

    private OperatorInput getInput(String notation) {
        return PortNotation.parse(notation)
                .find(operators, Operator::getInputs)
                .orElseThrow(() -> new IllegalArgumentException(MessageFormat.format(
                        "missing input: {0}",
                        notation)));
    }

    private OperatorOutput getOutput(String notation) {
        return PortNotation.parse(notation)
                .find(operators, Operator::getOutputs)
                .orElseThrow(() -> new IllegalArgumentException(MessageFormat.format(
                        "missing output: {0}",
                        notation)));
    }

    private static final class PortNotation {

        final String operatorId;

        final String portName;

        final int portNumber;

        private PortNotation(String operatorId, String portName, int portNumber) {
            this.operatorId = operatorId;
            this.portName = portName;
            this.portNumber = portNumber;
        }

        <P extends OperatorPort> Optional<P> find(
                Map<String, Operator> operators,
                Function<Operator, List<P>> members) {
            return Optionals.get(operators, operatorId)
                .map(members)
                .flatMap(ps -> {
                    if (portName != null) {
                        return ps.stream()
                                .filter(p -> p.getName().equals(portName))
                                .findFirst();
                    } else if (portNumber >= 0) {
                        if (portNumber < ps.size()) {
                            return Optionals.of(ps.get(0));
                        } else {
                            return Optionals.empty();
                        }
                    } else {
                        if (ps.size() == 1) {
                            return Optionals.of(ps.get(0));
                        } else {
                            return Optionals.empty();
                        }
                    }
                });
        }

        static PortNotation parse(String notation) {
            int at = notation.indexOf('.');
            if (at < 0) {
                return new PortNotation(notation, null, -1);
            }
            String op = notation.substring(0, at);
            String suffix = notation.substring(at + 1);
            if (suffix.isEmpty()) {
                return new PortNotation(op, null, -1);
            }
            char first = suffix.charAt(0);
            if ('0' <= first && first <= '9') {
                int number = Integer.parseInt(suffix);
                return new PortNotation(op, null, number);
            } else {
                return new PortNotation(op, suffix, -1);
            }
        }
    }
}
