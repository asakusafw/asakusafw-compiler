package com.asakusafw.lang.compiler.model.description;

/**
 * Represents a type.
 */
public interface TypeDescription extends Description {

    /**
     * Returns the type kind.
     * @return the type kind
     */
    TypeKind getTypeKind();

    /**
     * Represents a kind of {@link TypeDescription}.
     */
    public static enum TypeKind {

        /**
         * basic types.
         * @see BasicTypeDescription
         */
        BASIC,

        /**
         * array type.
         * @see ArrayTypeDescription
         */
        ARRAY,

        /**
         * class or interface types.
         * @see ClassDescription
         */
        CLASS,
    }
}
