package com.asakusafw.lang.compiler.model.description;

/**
 * Represents a value.
 * @see Descriptions
 */
public interface ValueDescription extends Description {

    /**
     * Returns the value kind.
     * @return the value kind
     */
    ValueKind getValueKind();

    /**
     * Returns the value type.
     * @return the value type
     */
    ReifiableTypeDescription getValueType();

    /**
     * Resolves this value.
     * @param classLoader the class loader
     * @return the resolved value
     * @throws ReflectiveOperationException if failed to resolve this value
     */
    Object resolve(ClassLoader classLoader) throws ReflectiveOperationException;

    /**
     * Represents a kind of value.
     */
    public static enum ValueKind {

        /**
         * immediate values.
         */
        IMMEDIATE,

        /**
         * {@code enum} constants.
         */
        ENUM_CONSTANT,

        /**
         * types.
         * @see ReifiableTypeDescription
         */
        TYPE,

        /**
         * annotations.
         * @see AnnotationDescription
         */
        ANNOTATION,

        /**
         * serializable values.
         * @see SerializableValueDescription
         */
        SERIALIZABLE,

        /**
         * array of other values.
         * @see ArrayDescription
         */
        ARRAY,

        /**
         * unknown values.
         * @see UnknownValueDescription
         */
        UNKNOWN,
    }
}
