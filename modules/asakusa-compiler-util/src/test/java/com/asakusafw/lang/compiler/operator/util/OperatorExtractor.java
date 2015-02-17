package com.asakusafw.lang.compiler.operator.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.graph.UserOperator;

/**
 * Mock.
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
                "missing @{0} {1}#{2}()",
                annotationType.getSimpleName(),
                operatorClass.getSimpleName(),
                methodName));
    }
}