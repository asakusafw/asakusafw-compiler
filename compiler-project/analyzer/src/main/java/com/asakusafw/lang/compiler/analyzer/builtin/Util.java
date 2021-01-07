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
package com.asakusafw.lang.compiler.analyzer.builtin;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputAttribute;

final class Util {

    static final Logger LOG = LoggerFactory.getLogger(Util.class);

    private Util() {
        return;
    }

    public static <T extends Enum<T>> T element(AnnotationDescription annotation, String elementName, T defaultValue) {
        ValueDescription element = annotation.getElements().get(elementName);
        if (element == null) {
            LOG.warn(MessageFormat.format(
                    "missing annotation element: @{0}({1})",
                    annotation.getDeclaringClass().getClassName(),
                    elementName));
            return defaultValue;
        }
        try {
            Class<T> type = defaultValue.getDeclaringClass();
            Object value = element.resolve(type.getClassLoader());
            if (type.isInstance(value)) {
                return type.cast(value);
            } else {
                LOG.warn(MessageFormat.format(
                        "invalid annotation element: {0} (expected {1})",
                        element,
                        type.getName()));
                return defaultValue;
            }
        } catch (ReflectiveOperationException e) {
            LOG.warn(MessageFormat.format(
                    "failed to resolve annotation element: @{0}({1})",
                    annotation.getDeclaringClass().getClassName(),
                    elementName), e);
            return defaultValue;
        }
    }

    public static ClassDescription getAnnotationType(Operator operator) {
        switch (operator.getOperatorKind()) {
        case CORE:
            return ((CoreOperator) operator).getCoreOperatorKind().getAnnotationType();
        case USER:
            return ((UserOperator) operator).getAnnotation().getDeclaringClass();
        default:
            throw new AssertionError(operator);
        }
    }

    public static <T extends Enum<T>> T resolve(CompilerOptions options, String key, T[] elements, T defaultElement) {
        String optionValue = options.get(key, null);
        if (optionValue != null) {
            T value = resolve(elements, optionValue);
            if (value == null) {
                LOG.warn(MessageFormat.format(
                        "invalid compiler option: {0}={1} (options: {2})",
                        key, optionValue, Arrays.toString(elements)));
            } else {
                return value;
            }
        }
        return defaultElement;
    }

    public static <T extends Enum<T>> T resolve(T[] elements, String value) {
        if (value == null) {
            return null;
        }
        for (T option : elements) {
            if (option.name().equals(value.toUpperCase())) {
                return option;
            }
        }
        return null;
    }

    public static OperatorClass resolve(OperatorClass.Builder builder, Operator operator) {
        OperatorClass result = builder.build();
        validate(result);
        return result;
    }

    private static void validate(OperatorClass info) {
        for (OperatorInput port : info.getInputs()) {
            Set<InputAttribute> attrs = info.getAttributes(port);
            if (attrs.contains(InputAttribute.PRIMARY) && attrs.contains(InputAttribute.JOIN_TABLE)) {
                throw new IllegalStateException();
            }
        }
    }
}
