/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.codegen;

import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;

import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.Context;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Composite implementation of {@link OperatorNodeGenerator}.
 * @since 0.4.0
 */
public class CompositeOperatorNodeGenerator {

    private static final String CATEGORY_NAME = "operator";

    private final Map<ClassDescription, OperatorNodeGenerator> elements;

    /**
     * Creates a new instance.
     * @param elements the elements
     */
    public CompositeOperatorNodeGenerator(Iterable<? extends OperatorNodeGenerator> elements) {
        Arguments.requireNonNull(elements);
        Map<ClassDescription, OperatorNodeGenerator> results = new LinkedHashMap<>();
        for (OperatorNodeGenerator element : elements) {
            results.put(element.getAnnotationType(), element);
        }
        this.elements = results;
    }

    /**
     * Loads elements via SPI.
     * @param classLoader the service class loader
     * @return the loaded object
     */
    public static CompositeOperatorNodeGenerator load(ClassLoader classLoader) {
        Arguments.requireNonNull(classLoader);
        return new CompositeOperatorNodeGenerator(ServiceLoader.load(OperatorNodeGenerator.class, classLoader));
    }

    /**
     * Generates a class for processing the operator, and returns the generated class binary.
     * @param context the current context
     * @param operator the target operator
     * @return the generated node info
     */
    public NodeInfo generate(Context context, Operator operator) {
        ClassDescription annotation;
        if (operator.getOperatorKind() == OperatorKind.CORE) {
            annotation = ((CoreOperator) operator).getCoreOperatorKind().getAnnotationType();
        } else if (operator.getOperatorKind() == OperatorKind.USER) {
            annotation = ((UserOperator) operator).getAnnotation().getDeclaringClass();
        } else {
            throw new IllegalArgumentException();
        }
        OperatorNodeGenerator element = elements.get(annotation);
        if (element == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "unsupported operator type: {0}",
                    annotation.getClassName()));
        }
        return element.generate(
                context,
                operator,
                () -> context.getClassName(CATEGORY_NAME, annotation.getSimpleName()));
    }
}
