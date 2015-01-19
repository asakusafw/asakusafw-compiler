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
                "UserOperator({1}#{2}@{0})", //$NON-NLS-1$
                method.getDeclaringClass().getName(),
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
