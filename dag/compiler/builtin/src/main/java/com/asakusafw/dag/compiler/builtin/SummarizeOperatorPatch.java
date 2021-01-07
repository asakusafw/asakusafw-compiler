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
package com.asakusafw.dag.compiler.builtin;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.asakusafw.lang.compiler.analyzer.util.OperatorUtil;
import com.asakusafw.lang.compiler.analyzer.util.PropertyFolding;
import com.asakusafw.lang.compiler.analyzer.util.PropertyFolding.Aggregation;
import com.asakusafw.lang.compiler.analyzer.util.SummarizedModelUtil;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.optimizer.OperatorRewriter;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.vocabulary.operator.Summarize;

/**
 * Rebuild {@link Group} info in {@link Summarize} operators.
 * @since 0.4.0
 */
public class SummarizeOperatorPatch implements OperatorRewriter {

    @Override
    public void perform(Context context, OperatorGraph graph) {
        for (Operator operator : graph.getOperators(true)) {
            if (SummarizedModelUtil.isSupported(operator) == false) {
                continue;
            }
            if (operator.getAttribute(Patched.class) == Patched.INSTANCE) {
                // already patched
                continue;
            }
            Operator patched = patch(context, (UserOperator) operator);
            Operators.replace(operator, patched);
            graph.remove(operator);
            graph.add(patched);
        }
    }

    private static Operator patch(Context context, UserOperator operator) {
        OperatorUtil.checkOperatorPorts(operator, 1, 1);
        Invariants.require(operator.getInputs().size() == 1);
        Invariants.require(operator.getOutputs().size() == 1);
        Invariants.require(operator.getArguments().isEmpty());
        OperatorInput input = operator.getInput(Summarize.ID_INPUT);
        OperatorOutput output = operator.getOutput(Summarize.ID_OUTPUT);
        UserOperator.Builder builder = UserOperator.builder(
                operator.getAnnotation(),
                operator.getMethod(),
                operator.getImplementationClass());
        builder.constraint(operator.getConstraints());
        builder.input(input.getName(), input.getDataType(), rebuildInputGroup(context, input));
        builder.output(output.getName(), output.getDataType());
        for (Class<?> attributeType : operator.getAttributeTypes()) {
            putAttribute(builder, operator, attributeType);
        }
        builder.attribute(Patched.class, Patched.INSTANCE);
        return builder.build();
    }

    private static <T> void putAttribute(UserOperator.Builder target, UserOperator source, Class<T> attributeType) {
        target.attribute(attributeType, source.getAttribute(attributeType));
    }

    private static Group rebuildInputGroup(Context context, OperatorInput input) {
        List<PropertyFolding> foldings;
        try {
            foldings = SummarizedModelUtil.getPropertyFoldings(context.getClassLoader(), input.getOwner());
        } catch (ReflectiveOperationException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "error occurred while analyzing operator: {0}",
                    input.getOwner()),
                    e);
        }
        Map<PropertyName, PropertyName> map = foldings.stream()
                .filter(f -> f.getAggregation() == Aggregation.ANY)
                .map(f -> f.getMapping())
                .collect(Collectors.toMap(m -> m.getSourceProperty(), m -> m.getDestinationProperty()));

        Group origin = input.getGroup();
        Invariants.requireNonNull(origin);
        Group patched = new Group(
                Lang.project(origin.getGrouping(), n -> Invariants.requireNonNull(map.get(n))),
                Collections.emptyList());
        return patched;
    }

    private static final class Patched {

        static final Patched INSTANCE = new Patched();

        @Override
        public String toString() {
            return "true";
        }
    }
}
