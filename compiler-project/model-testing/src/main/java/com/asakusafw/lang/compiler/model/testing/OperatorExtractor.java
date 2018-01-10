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
package com.asakusafw.lang.compiler.model.testing;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.graph.UserOperator;

/**
 * Extracts operator from declaring operator methods.
 */
public final class OperatorExtractor {

    private OperatorExtractor() {
        return;
    }

    /**
     * Returns user operator builder for the specified method.
     * @param annotationType the annotation type
     * @param operatorClass the operator class
     * @param methodName the target method name
     * @return the user operator builder
     */
    public static UserOperator.Builder extract(
            Class<? extends Annotation> annotationType,
            Class<?> operatorClass,
            String methodName) {
        Method method = findMethod(annotationType, operatorClass, methodName);
        Annotation annotation = method.getAnnotation(annotationType);
        assert annotation != null;
        return UserOperator.builder(
                AnnotationDescription.of(annotation),
                MethodDescription.of(method),
                ClassDescription.of(operatorClass));
    }

    private static Method findMethod(
            Class<? extends Annotation> annotationType,
            Class<?> operatorClass,
            String methodName) {
        for (Method method : operatorClass.getMethods()) {
            if (method.getName().equals(methodName) == false) {
                continue;
            }
            if (method.isAnnotationPresent(annotationType) == false) {
                continue;
            }
            return method;
        }
        throw new AssertionError(MessageFormat.format(
                "missing @{0} {1}#{2}()", //$NON-NLS-1$
                annotationType.getSimpleName(),
                operatorClass.getSimpleName(),
                methodName));
    }
}
