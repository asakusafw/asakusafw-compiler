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
package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;

/**
 * Represents a user defined operator.
 */
public final class UserOperator extends Operator {

    private final AnnotationDescription annotation;

    private final MethodDescription method;

    private final ClassDescription implementationClass;

    private UserOperator(
            AnnotationDescription annotation,
            MethodDescription method,
            ClassDescription implementationClass) {
        this.annotation = annotation;
        this.method = method;
        this.implementationClass = implementationClass;
    }

    @Override
    public UserOperator copy() {
        return copyAttributesTo(new UserOperator(annotation, method, implementationClass));
    }

    /**
     * Creates a new builder.
     * @param annotation the operator annotation
     * @param method the operator method
     * @param implementationClass the operator implementation class
     * @return the builder
     */
    public static Builder builder(
            AnnotationDescription annotation,
            MethodDescription method,
            ClassDescription implementationClass) {
        return new Builder(new UserOperator(annotation, method, implementationClass));
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.USER;
    }

    /**
     * Returns the operator annotation.
     * @return the operator annotation
     */
    public AnnotationDescription getAnnotation() {
        return annotation;
    }

    /**
     * Returns the operator method.
     * Note that, it may be {@code abstract} method.
     * @return the operator method
     */
    public MethodDescription getMethod() {
        return method;
    }

    /**
     * Returns the operator implementation class.
     * @return the operator implementation class
     */
    public ClassDescription getImplementationClass() {
        return implementationClass;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "UserOperator({2}:{1}@{0})", //$NON-NLS-1$
                method.getDeclaringClass().getClassName(),
                method.getName(),
                annotation.getDeclaringClass().getSimpleName());
    }

    /**
     * A builder for {@link UserOperator}.
     */
    public static final class Builder extends AbstractBuilder<UserOperator, Builder> {

        Builder(UserOperator owner) {
            super(owner);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }
    }
}
