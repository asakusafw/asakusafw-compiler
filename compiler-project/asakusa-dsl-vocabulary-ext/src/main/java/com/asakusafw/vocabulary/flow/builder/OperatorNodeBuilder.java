/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.vocabulary.flow.builder;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

import com.asakusafw.vocabulary.flow.graph.FlowElementAttribute;
import com.asakusafw.vocabulary.flow.graph.FlowElementDescription;
import com.asakusafw.vocabulary.flow.graph.OperatorDescription;

/**
 * Builds operator internally.
 */
public class OperatorNodeBuilder extends FlowElementBuilder {

    private final Class<? extends Annotation> annotationType;

    private final Class<?> implementationClass;

    private final Method method;

    /**
     * Creates a new instance for operator method.
     * @param annotationType operator annotation type.
     * @param operatorClass operator class
     * @param implementationClass operator implementation class
     * @param methodName operator method name
     * @param methodParameterTypes operator method parameter types (erasure)
     * @throws IllegalArgumentException if some parameters were {@code null}
     */
    public OperatorNodeBuilder(
            Class<? extends Annotation> annotationType,
            Class<?> operatorClass,
            Class<?> implementationClass,
            String methodName,
            Class<?>... methodParameterTypes) {
        if (annotationType == null) {
            throw new IllegalArgumentException("annotationType must not be null"); //$NON-NLS-1$
        }
        if (operatorClass == null) {
            throw new IllegalArgumentException("operatorClass must not be null"); //$NON-NLS-1$
        }
        if (implementationClass == null) {
            throw new IllegalArgumentException("implementationClass must not be null"); //$NON-NLS-1$
        }
        if (methodName == null) {
            throw new IllegalArgumentException("methodName must not be null"); //$NON-NLS-1$
        }
        if (methodParameterTypes == null) {
            throw new IllegalArgumentException("methodParameters must not be null"); //$NON-NLS-1$
        }
        if (operatorClass.isAssignableFrom(implementationClass) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "implementationClass ({0}) must be subclass of operatorClass ({1})",
                    implementationClass.getName(),
                    operatorClass.getName()));
        }
        this.annotationType = annotationType;
        this.implementationClass = implementationClass;
        try {
            this.method = operatorClass.getMethod(methodName, methodParameterTypes);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(MessageFormat.format(
                    "Failed to detect operator method (class={0}, name={1}, parameters={2})",
                    operatorClass.getName(),
                    methodName,
                    Arrays.toString(methodParameterTypes)), e);
        }
    }

    @Override
    protected FlowElementDescription build(
            List<PortInfo> inputPorts,
            List<PortInfo> outputPorts,
            List<DataInfo> arguments,
            List<AttributeInfo> attributes) {
        OperatorDescription.Builder builder = new OperatorDescription.Builder(annotationType);
        builder.declare(method.getDeclaringClass(), implementationClass, method.getName());
        for (Class<?> type : method.getParameterTypes()) {
            builder.declareParameter(type);
        }
        for (PortInfo info : inputPorts) {
            if (info.getExtern() != null) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "operator cannot accept external input: {0}#{1}({2})",
                        method.getDeclaringClass().getName(),
                        method.getName(),
                        info.getName()));
            }
            if (info.getKey() != null) {
                builder.addInput(info.getName(), info.getType(), info.getKey().toShuffleKey());
            } else {
                builder.addInput(info.getName(), info.getType());
            }
        }
        for (PortInfo info : outputPorts) {
            if (info.getKey() != null) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "operator cannot accept shuffle key: {0}#{1}({2})",
                        method.getDeclaringClass().getName(),
                        method.getName(),
                        info.getName()));
            }
            if (info.getExtern() != null) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "operator cannot accept external output: {0}#{1}({2})",
                        method.getDeclaringClass().getName(),
                        method.getName(),
                        info.getName()));
            }
            builder.addOutput(info.getName(), info.getType());
        }
        for (DataInfo info : arguments) {
            Data data = info.getData();
            switch (data.getKind()) {
            case CONSTANT: {
                Constant c = (Constant) data;
                builder.addParameter(info.getName(), c.getType(), c.getValue());
                break;
            }
            default:
                throw new AssertionError(data);
            }
        }
        for (AttributeInfo attribute : attributes) {
            builder.addAttribute(attribute.getAdapter(FlowElementAttribute.class));
        }
        return builder.toDescription();
    }
}
