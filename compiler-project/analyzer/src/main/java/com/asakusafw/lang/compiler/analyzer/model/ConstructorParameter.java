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
package com.asakusafw.lang.compiler.analyzer.model;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.Objects;

/**
 * Represents a constructor parameter.
 * @since 0.3.0
 */
public class ConstructorParameter extends AbstractAnnotatedElement {

    private final Constructor<?> constructor;

    private final int parameterIndex;

    private final Type type;

    /**
     * Creates a new instance.
     * @param constructor the target constructor
     * @param parameterIndex the target parameter index in the constructor
     */
    public ConstructorParameter(Constructor<?> constructor, int parameterIndex) {
        super(pick(constructor, parameterIndex));
        this.constructor = constructor;
        this.parameterIndex = parameterIndex;
        this.type = constructor.getGenericParameterTypes()[parameterIndex];
    }

    private static Annotation[] pick(Constructor<?> constructor, int parameterIndex) {
        Objects.requireNonNull(constructor);
        Class<?>[] parameterTypes = constructor.getParameterTypes();
        if (parameterIndex < 0 || parameterIndex >= parameterTypes.length) {
            throw new IllegalArgumentException();
        }
        return constructor.getParameterAnnotations()[parameterIndex];
    }

    /**
     * Returns the constructor.
     * @return the constructor
     */
    public Constructor<?> getConstructor() {
        return constructor;
    }

    /**
     * Returns the parameter index.
     * @return the parameter index
     */
    public int getParameterIndex() {
        return parameterIndex;
    }

    /**
     * Returns the parameter type.
     * @return the type
     */
    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "{0}({1})", //$NON-NLS-1$
                constructor.getName(),
                parameterIndex);
    }
}
