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
package com.asakusafw.lang.compiler.analyzer.util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.asakusafw.lang.compiler.analyzer.util.PropertyFolding.Aggregation;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ReifiableTypeDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.vocabulary.model.Summarized;
import com.asakusafw.vocabulary.operator.Summarize;

/**
 * Utilities for <em>summarized models</em>.
 */
public final class SummarizedModelUtil {

    private static final ClassDescription SUMMARIZE = Descriptions.classOf(Summarize.class);

    private static final Set<ClassDescription> SUPPORTED;
    static {
        Set<ClassDescription> set = new HashSet<>();
        set.add(SUMMARIZE);
        SUPPORTED = set;
    }

    private SummarizedModelUtil() {
        return;
    }

    /**
     * Returns whether the target class represents a <em>summarized model</em>.
     * @param dataModelType the target data model type
     * @return {@code true} if the target class represents a <em>summarized model</em>, otherwise {@code false}
     */
    public static boolean isSupported(Class<?> dataModelType) {
        return dataModelType.isAnnotationPresent(Summarized.class);
    }

    /**
     * Returns whether the target operator is using <em>summarized models</em>.
     * @param operator the target operator
     * @return {@code true} if the target operator is using <em>summarized models</em>, otherwise {@code false}
     */
    public static boolean isSupported(Operator operator) {
        if (operator.getOperatorKind() != OperatorKind.USER) {
            return false;
        }
        UserOperator op = (UserOperator) operator;
        AnnotationDescription annotation = op.getAnnotation();
        return SUPPORTED.contains(annotation.getDeclaringClass());
    }

    /**
     * Extracts property mappings from an operator with <em>summarized models</em>.
     * @param classLoader the class loader to resolve target operator
     * @param operator the target operator
     * @return the property mappings
     * @throws ReflectiveOperationException if failed to resolve operators
     * @throws IllegalArgumentException if the target operator is not supported
     */
    public static List<PropertyFolding> getPropertyFoldings(
            ClassLoader classLoader,
            Operator operator) throws ReflectiveOperationException {
        if (isSupported(operator) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "operator must be using summarized models: {0}",
                    operator));
        }
        return analyzeSummarize(classLoader, (UserOperator) operator);
    }

    private static List<PropertyFolding> analyzeSummarize(
            ClassLoader classLoader,
            UserOperator operator) throws ClassNotFoundException {
        OperatorUtil.checkOperatorPorts(operator, 1, 1);
        OperatorInput input = operator.getInput(Summarize.ID_INPUT);
        OperatorOutput summarized = operator.getOutput(Summarize.ID_OUTPUT);
        return analyzeSummarize(classLoader, operator, input, summarized);
    }

    private static List<PropertyFolding> analyzeSummarize(
            ClassLoader classLoader,
            UserOperator operator,
            OperatorInput input, OperatorOutput summarized) throws ClassNotFoundException {
        List<FoldingElement> elements = analyzeSummarizedModel(classLoader, summarized.getDataType());
        List<PropertyFolding> results = new ArrayList<>();
        for (FoldingElement element : elements) {
            PropertyMapping mapping = new PropertyMapping(input, element.source, summarized, element.destination);
            results.add(new PropertyFolding(mapping, element.aggregation));
        }
        return results;
    }

    private static List<FoldingElement> analyzeSummarizedModel(
            ClassLoader classLoader,
            TypeDescription dataType) throws ClassNotFoundException {
        if ((dataType instanceof ReifiableTypeDescription) == false) {
            throw new IllegalStateException(MessageFormat.format(
                    "class must be a summarized data model: {0}",
                    dataType));
        }
        Class<?> dataModelClass = ((ReifiableTypeDescription) dataType).resolve(classLoader);
        Summarized annotation = dataModelClass.getAnnotation(Summarized.class);
        if (annotation == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "class must be a summarized data model: {0}",
                    dataModelClass.getName()));
        }
        return analyzeAnntation(annotation);
    }

    private static List<FoldingElement> analyzeAnntation(Summarized annotation) {
        List<FoldingElement> results = new ArrayList<>();
        for (Summarized.Folding folding : annotation.term().foldings()) {
            PropertyName source = PropertyName.of(folding.source());
            PropertyName destination = PropertyName.of(folding.destination());
            PropertyFolding.Aggregation aggregation = convert(folding.aggregator());
            results.add(new FoldingElement(source, destination, aggregation));
        }
        return results;
    }

    private static Aggregation convert(Summarized.Aggregator aggregator) {
        switch (aggregator) {
        case ANY:
            return PropertyFolding.Aggregation.ANY;
        case COUNT:
            return PropertyFolding.Aggregation.COUNT;
        case MAX:
            return PropertyFolding.Aggregation.MAX;
        case MIN:
            return PropertyFolding.Aggregation.MIN;
        case SUM:
            return PropertyFolding.Aggregation.SUM;
        default:
            throw new AssertionError(aggregator);
        }
    }

    private static final class FoldingElement {

        final PropertyName source;

        final PropertyName destination;

        final PropertyFolding.Aggregation aggregation;

        FoldingElement(PropertyName source, PropertyName destination, PropertyFolding.Aggregation aggregation) {
            this.source = source;
            this.destination = destination;
            this.aggregation = aggregation;
        }
    }
}
