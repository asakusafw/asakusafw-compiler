/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.info;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CustomOperator;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorArgument;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.info.api.AttributeCollector;
import com.asakusafw.lang.info.graph.Input;
import com.asakusafw.lang.info.graph.Node;
import com.asakusafw.lang.info.graph.Output;
import com.asakusafw.lang.info.operator.CoreOperatorSpec;
import com.asakusafw.lang.info.operator.CustomOperatorSpec;
import com.asakusafw.lang.info.operator.FlowOperatorSpec;
import com.asakusafw.lang.info.operator.InputAttribute;
import com.asakusafw.lang.info.operator.InputGranularity;
import com.asakusafw.lang.info.operator.InputGroup;
import com.asakusafw.lang.info.operator.InputOperatorSpec;
import com.asakusafw.lang.info.operator.MarkerOperatorSpec;
import com.asakusafw.lang.info.operator.OperatorAttribute;
import com.asakusafw.lang.info.operator.OperatorGraphAttribute;
import com.asakusafw.lang.info.operator.OperatorSpec;
import com.asakusafw.lang.info.operator.OutputAttribute;
import com.asakusafw.lang.info.operator.OutputOperatorSpec;
import com.asakusafw.lang.info.operator.ParameterInfo;
import com.asakusafw.lang.info.operator.UserOperatorSpec;

/**
 * Collects {@link OperatorGraphAttribute}.
 * @since 0.4.2
 */
public class OperatorGraphAttributeCollector implements AttributeCollector {

    @Override
    public void process(Context context, Jobflow jobflow) {
        Node root = new Node();
        process(context, jobflow.getOperatorGraph(), root);
        context.putAttribute(new OperatorGraphAttribute(root));
    }

    static void process(Context context, OperatorGraph graph, Node info) {
        Map<OperatorInput, Input> downstreams = new HashMap<>();
        Map<OperatorOutput, Output> upstreams = new HashMap<>();
        Collection<Operator> operators = sort(graph);
        for (Operator operator : operators) {
            List<ParameterInfo> parameters = new ArrayList<>();
            Node node = info.newElement();
            for (OperatorInput input : operator.getInputs()) {
                InputAttribute attr = convert(input);
                Input result = node.newInput().withAttribute(attr);
                downstreams.put(input, result);
            }
            for (OperatorOutput output : operator.getOutputs()) {
                OutputAttribute attr = convert(output);
                Output result = node.newOutput().withAttribute(attr);
                upstreams.put(output, result);
            }
            for (OperatorArgument argument : operator.getArguments()) {
                ParameterInfo attr = convert(argument);
                parameters.add(attr);
            }
            OperatorSpec spec = convert(operator);
            node.withAttribute(new OperatorAttribute(spec, parameters));
            if (operator.getOperatorKind() == Operator.OperatorKind.FLOW) {
                process(context, ((FlowOperator) operator).getOperatorGraph(), node);
            }
        }
        for (Operator operator : operators) {
            for (OperatorInput downstream : operator.getInputs()) {
                for (OperatorOutput upstream : downstream.getOpposites()) {
                    Input d = downstreams.get(downstream);
                    Output u = upstreams.get(upstream);
                    if (d != null && u != null) {
                        d.connect(u);
                    }
                }
            }
        }
    }

    private static Collection<Operator> sort(OperatorGraph graph) {
        // TODO sort operators
        return graph.getOperators(false);
    }

    private static OperatorSpec convert(Operator operator) {
        switch (operator.getOperatorKind()) {
        case CORE:
            return convert0((CoreOperator) operator);
        case USER:
            return convert0((UserOperator) operator);
        case FLOW:
            return convert0((FlowOperator) operator);
        case INPUT:
            return convert0((ExternalInput) operator);
        case OUTPUT:
            return convert0((ExternalOutput) operator);
        case MARKER:
            return convert0((MarkerOperator) operator);
        case CUSTOM:
            return convert0((CustomOperator) operator);
        default:
            throw new AssertionError(operator);
        }
    }

    private static CoreOperatorSpec convert0(CoreOperator operator) {
        return CoreOperatorSpec.of(translate(operator.getCoreOperatorKind()));
    }

    private static CoreOperatorSpec.CoreOperatorKind translate(CoreOperator.CoreOperatorKind kind) {
        switch (kind) {
        case CHECKPOINT:
            return CoreOperatorSpec.CoreOperatorKind.CHECKPOINT;
        case EXTEND:
            return CoreOperatorSpec.CoreOperatorKind.EXTEND;
        case PROJECT:
            return CoreOperatorSpec.CoreOperatorKind.PROJECT;
        case RESTRUCTURE:
            return CoreOperatorSpec.CoreOperatorKind.RESTRUCTURE;
        default:
            throw new AssertionError(kind);
        }
    }

    private static UserOperatorSpec convert0(UserOperator operator) {
        return UserOperatorSpec.of(
                Util.convert(operator.getAnnotation()),
                Util.convert(operator.getMethod().getDeclaringClass()),
                Util.convert(operator.getImplementationClass()),
                operator.getMethod().getName());
    }

    private static FlowOperatorSpec convert0(FlowOperator operator) {
        return FlowOperatorSpec.of(
                Optional.ofNullable(operator.getDescriptionClass())
                    .map(Util::convert)
                    .orElse(null));
    }

    private static InputOperatorSpec convert0(ExternalInput operator) {
        return InputOperatorSpec.of(
                operator.getName(),
                Optional.ofNullable(operator.getInfo())
                    .flatMap(it -> Optional.ofNullable(it.getDescriptionClass()))
                    .map(Util::convert)
                    .orElse(null));
    }

    private static OutputOperatorSpec convert0(ExternalOutput operator) {
        return OutputOperatorSpec.of(
                operator.getName(),
                Optional.ofNullable(operator.getInfo())
                    .flatMap(it -> Optional.ofNullable(it.getDescriptionClass()))
                    .map(Util::convert)
                    .orElse(null));
    }

    private static MarkerOperatorSpec convert0(MarkerOperator operator) {
        return MarkerOperatorSpec.get();
    }

    private static CustomOperatorSpec convert0(CustomOperator operator) {
        return CustomOperatorSpec.of(operator.getCategory());
    }

    private static InputAttribute convert(OperatorInput input) {
        return new InputAttribute(
                input.getName(),
                Util.convert(input.getDataType()),
                translate(input.getInputUnit()),
                translate(input.getGroup()));
    }

    private static InputGranularity translate(OperatorInput.InputUnit kind) {
        switch (kind) {
        case RECORD:
            return InputGranularity.RECORD;
        case GROUP:
            return InputGranularity.RECORD;
        case WHOLE:
            return InputGranularity.RECORD;
        default:
            throw new AssertionError(kind);
        }
    }

    private static InputGroup translate(Group group) {
        if (group == null) {
            return null;
        }
        return new InputGroup(
                group.getGrouping().stream()
                    .map(PropertyName::toName)
                    .collect(Collectors.toList()),
                group.getOrdering().stream()
                    .map(it -> new InputGroup.Order(it.getPropertyName().toName(), translate(it.getDirection())))
                    .collect(Collectors.toList()));
    }

    private static InputGroup.Direction translate(Group.Direction kind) {
        switch (kind) {
        case ASCENDANT:
            return InputGroup.Direction.ASCENDANT;
        case DESCENDANT:
            return InputGroup.Direction.ASCENDANT;
        default:
            throw new AssertionError(kind);
        }
    }

    private static OutputAttribute convert(OperatorOutput output) {
        return new OutputAttribute(
                output.getName(),
                Util.convert(output.getDataType()));
    }

    private static ParameterInfo convert(OperatorArgument argument) {
        return new ParameterInfo(
                argument.getName(),
                Util.convert(argument.getValue().getValueType()),
                Util.convert(argument.getValue()));
    }
}
