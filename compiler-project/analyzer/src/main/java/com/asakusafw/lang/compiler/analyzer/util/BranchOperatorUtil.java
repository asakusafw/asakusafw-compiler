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

import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.vocabulary.operator.Branch;
import com.asakusafw.vocabulary.operator.MasterBranch;

/**
 * Utilities for <em>branch kind</em> operator.
 */
public final class BranchOperatorUtil {

    private static final Set<ClassDescription> SUPPORTED;
    static {
        Set<ClassDescription> set = new HashSet<>();
        set.add(Descriptions.classOf(Branch.class));
        set.add(Descriptions.classOf(MasterBranch.class));
        SUPPORTED = set;
    }

    private BranchOperatorUtil() {
        return;
    }

    /**
     * Returns whether the target operator is <em>branch kind</em> or not.
     * @param operator the target operator
     * @return {@code true} if the target operator is <em>branch kind</em>, otherwise {@code false}
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
     * Extracts operator outputs and find their corresponding enum constants.
     * @param classLoader the class loader to resolve target operator
     * @param operator the target operator
     * @return the operator output and its corresponding enum constant
     * @throws ReflectiveOperationException if failed to resolve operators
     * @throws IllegalArgumentException if the target operator is not supported
     */
    public static Map<OperatorOutput, Enum<?>> getOutputMap(
            ClassLoader classLoader,
            Operator operator) throws ReflectiveOperationException {
        if (isSupported(operator) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "operator must be a kind of Brahch: {0}",
                    operator));
        }
        UserOperator user = (UserOperator) operator;
        MethodDescription description = user.getMethod();
        Method method = description.resolve(classLoader);
        Class<?> returnType = method.getReturnType();
        if (Enum.class.isAssignableFrom(returnType) == false) {
            throw new IllegalStateException(MessageFormat.format(
                    "Branch kind operator method must return Enum object: {1} ({0})",
                    returnType.getName(),
                    description));
        }
        Enum<?>[] constants = (Enum<?>[]) returnType.getEnumConstants();
        Map<PropertyName, OperatorOutput> outputs = createOutputMap(operator);
        Map<OperatorOutput, Enum<?>> results = new LinkedHashMap<>();
        for (Enum<?> c : constants) {
            PropertyName name = PropertyName.of(c.name());
            OperatorOutput output = outputs.remove(name);
            if (output == null) {
                throw new IllegalStateException(MessageFormat.format(
                        "failed to resolve branch output: {0} ({1}#{2} is missing)",
                        operator,
                        c.getDeclaringClass().getName(),
                        c.name()));
            }
            results.put(output, c);
        }
        if (outputs.isEmpty() == false) {
            throw new IllegalStateException(MessageFormat.format(
                    "failed to resolve branch output: {0} ({1} are not mapped missing)",
                    operator,
                    outputs.keySet()));
        }
        return results;
    }

    private static Map<PropertyName, OperatorOutput> createOutputMap(Operator operator) {
        Map<PropertyName, OperatorOutput> results = new LinkedHashMap<>();
        for (OperatorOutput output : operator.getOutputs()) {
            PropertyName name = PropertyName.of(output.getName());
            if (results.containsKey(name)) {
                throw new IllegalStateException(MessageFormat.format(
                        "conflict operator output: {0} (\"{1}\" <=> \"{2}\")",
                        operator,
                        output.getName(),
                        results.get(name).getName()));
            }
            results.put(name, output);
        }
        return results;
    }
}
