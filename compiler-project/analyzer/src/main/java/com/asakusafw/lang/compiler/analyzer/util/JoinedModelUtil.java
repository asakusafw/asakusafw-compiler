/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.vocabulary.model.Joined;
import com.asakusafw.vocabulary.operator.MasterJoin;
import com.asakusafw.vocabulary.operator.Split;

/**
 * Utilities for <em>joined models</em>.
 */
public final class JoinedModelUtil {

    private static final ClassDescription MASTER_JOIN = Descriptions.classOf(MasterJoin.class);

    private static final ClassDescription SPLIT = Descriptions.classOf(Split.class);

    private static final Set<ClassDescription> SUPPORTED;
    static {
        Set<ClassDescription> set = new HashSet<>();
        set.add(SPLIT);
        set.add(MASTER_JOIN);
        SUPPORTED = set;
    }

    private JoinedModelUtil() {
        return;
    }

    /**
     * Returns whether the target class represents a <em>joined model</em>.
     * @param dataModelType the target data model type
     * @return {@code true} if the target class represents a <em>joined model</em>, otherwise {@code false}
     */
    public static boolean isSupported(Class<?> dataModelType) {
        return dataModelType.isAnnotationPresent(Joined.class);
    }

    /**
     * Returns whether the target operator is using <em>joined models</em>.
     * @param operator the target operator
     * @return {@code true} if the target operator is using <em>joined models</em>, otherwise {@code false}
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
     * Extracts property mappings from an operator with <em>joined models</em>.
     * @param classLoader the class loader to resolve target operator
     * @param operator the target operator
     * @return the property mappings
     * @throws ReflectiveOperationException if failed to resolve operators
     * @throws IllegalArgumentException if the target operator is not supported
     */
    public static List<PropertyMapping> getPropertyMappings(
            ClassLoader classLoader,
            Operator operator) throws ReflectiveOperationException {
        if (isSupported(operator) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "operator must be using joined models: {0}",
                    operator));
        }
        UserOperator op = (UserOperator) operator;
        AnnotationDescription annotation = op.getAnnotation();
        ClassDescription operatorKind = annotation.getDeclaringClass();
        if (operatorKind.equals(MASTER_JOIN)) {
            return analyzeMasterJoin(classLoader, op);
        } else if (operatorKind.equals(SPLIT)) {
            return analyzeSplit(classLoader, op);
        } else {
            throw new AssertionError(operatorKind);
        }
    }

    private static List<PropertyMapping> analyzeMasterJoin(
            ClassLoader classLoader,
            UserOperator operator) throws ClassNotFoundException {
        OperatorUtil.checkOperatorPorts(operator, 2, 2);
        OperatorInput a = operator.getInput(MasterJoin.ID_INPUT_MASTER);
        OperatorInput b = operator.getInput(MasterJoin.ID_INPUT_TRANSACTION);
        OperatorOutput joined = operator.getOutput(MasterJoin.ID_OUTPUT_JOINED);
        return analyzeMasterJoin(classLoader, operator, a, b, joined);
    }

    private static List<PropertyMapping> analyzeSplit(
            ClassLoader classLoader,
            UserOperator operator) throws ClassNotFoundException {
        OperatorUtil.checkOperatorPorts(operator, 1, 2);
        OperatorInput joined = operator.getInput(Split.ID_INPUT);
        OperatorOutput a = operator.getOutput(Split.ID_OUTPUT_LEFT);
        OperatorOutput b = operator.getOutput(Split.ID_OUTPUT_RIGHT);
        return analyzeSplit(classLoader, operator, joined, a, b);
    }

    private static List<PropertyMapping> analyzeMasterJoin(
            ClassLoader classLoader,
            UserOperator operator,
            OperatorInput a, OperatorInput b, OperatorOutput joined) throws ClassNotFoundException {
        Map<OperatorInput, List<MappingElement>> mappings =
                analyzeJoinedModel(classLoader, joined.getDataType(), a, b);
        List<PropertyMapping> results = new ArrayList<>();
        Set<PropertyName> saw = new HashSet<>();
        for (Map.Entry<OperatorInput, List<MappingElement>> entry : mappings.entrySet()) {
            OperatorInput sourcePort = entry.getKey();
            for (MappingElement element : entry.getValue()) {
                if (saw.contains(element.destination)) {
                    continue;
                }
                saw.add(element.destination);
                results.add(new PropertyMapping(sourcePort, element.source, joined, element.destination));
            }
        }
        return results;
    }

    private static List<PropertyMapping> analyzeSplit(
            ClassLoader classLoader,
            UserOperator operator,
            OperatorInput joined, OperatorOutput a, OperatorOutput b) throws ClassNotFoundException {
        Map<OperatorOutput, List<MappingElement>> mappings =
                analyzeJoinedModel(classLoader, joined.getDataType(), a, b);
        List<PropertyMapping> results = new ArrayList<>();
        for (Map.Entry<OperatorOutput, List<MappingElement>> entry : mappings.entrySet()) {
            OperatorOutput destinationPort = entry.getKey();
            for (MappingElement element : entry.getValue()) {
                results.add(new PropertyMapping(joined, element.destination, destinationPort, element.source));
            }
        }
        return results;
    }

    private static <T extends OperatorPort> Map<T, List<MappingElement>> analyzeJoinedModel(
            ClassLoader classLoader,
            TypeDescription dataType, T a, T b) throws ClassNotFoundException {
        if ((dataType instanceof ReifiableTypeDescription) == false) {
            throw new IllegalStateException(MessageFormat.format(
                    "class must be a joined data model: {0}",
                    dataType));
        }
        Class<?> dataModelClass = ((ReifiableTypeDescription) dataType).resolve(classLoader);
        Joined annotation = dataModelClass.getAnnotation(Joined.class);
        if (annotation == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "class must be a joined data model: {0}",
                    dataModelClass.getName()));
        }
        Map<TypeDescription, List<MappingElement>> mappings = analyzeAnntation(annotation);
        Map<T, List<MappingElement>> results = new LinkedHashMap<>();
        for (T port : Arrays.asList(a, b)) {
            List<MappingElement> elements = mappings.remove(port.getDataType());
            if (elements == null) {
                throw new IllegalStateException(MessageFormat.format(
                        "invalid joined model {0} (must consist of {1}): {2}",
                        dataType,
                        port.getDataType(),
                        mappings.keySet()));
            }
            results.put(port, elements);
        }
        return results;
    }

    private static Map<TypeDescription, List<MappingElement>> analyzeAnntation(Joined annotation) {
        Map<TypeDescription, List<MappingElement>> results = new LinkedHashMap<>();
        for (Joined.Term term : annotation.terms()) {
            ReifiableTypeDescription type = Descriptions.typeOf(term.source());
            assert results.containsKey(type) == false;
            List<MappingElement> elements = new ArrayList<>();
            for (Joined.Mapping mapping : term.mappings()) {
                PropertyName source = PropertyName.of(mapping.source());
                PropertyName destination = PropertyName.of(mapping.destination());
                elements.add(new MappingElement(source, destination));
            }
            results.put(type, elements);
        }
        return results;
    }

    private static final class MappingElement {

        final PropertyName source;

        final PropertyName destination;

        MappingElement(PropertyName source, PropertyName destination) {
            this.source = source;
            this.destination = destination;
        }
    }
}
