package com.asakusafw.lang.compiler.model.description;

import java.io.Serializable;
import java.lang.annotation.Annotation;

/**
 * Utilities for {@link Description}.
 */
public final class Descriptions {

    private Descriptions() {
        return;
    }

    /**
     * Converts an object into related {@link ValueDescription}.
     * @param value the original value
     * @return the related {@link ValueDescription}
     */
    public static ValueDescription valueOf(Object value) {
        if (value == null) {
            // FIXME for null values?
            throw new IllegalArgumentException();
        }
        Class<?> aClass = value.getClass();
        if (ImmediateDescription.isBoxed(aClass) || value instanceof String) {
            return ImmediateDescription.of(value);
        } else if (value instanceof Class<?>) {
            return ReifiableTypeDescription.of((Class<?>) value);
        } else if (value instanceof Enum<?>) {
            return EnumConstantDescription.of((Enum<?>) value);
        } else if (value instanceof Annotation) {
            return AnnotationDescription.of((Annotation) value);
        } else if (aClass.isArray()) {
            return ArrayDescription.of(value);
        } else if (value instanceof Serializable) {
            return SerializableValueDescription.of(value);
        }
        return UnknownValueDescription.of(value);
    }

    /**
     * Converts a class object into related {@link ReifiableTypeDescription}.
     * @param aClass the reflective object
     * @see ReifiableTypeDescription#of(Class)
     * @return the related instance
     */
    public static ReifiableTypeDescription typeOf(Class<?> aClass) {
        return ReifiableTypeDescription.of(aClass);
    }

    /**
     * Converts a class object into related {@link ClassDescription}.
     * @param aClass the reflective object (must be class or interface type)
     * @return the related instance
     * @see ClassDescription#of(Class)
     * @see #typeOf(Class)
     */
    public static ClassDescription classOf(Class<?> aClass) {
        return ClassDescription.of(aClass);
    }
}
