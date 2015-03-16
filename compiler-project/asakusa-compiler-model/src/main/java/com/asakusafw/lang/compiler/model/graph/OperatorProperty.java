package com.asakusafw.lang.compiler.model.graph;

/**
 * A property of {@link Operator}.
 */
public interface OperatorProperty {

    /**
     * Returns the {@link PropertyKind}.
     * @return the {@link PropertyKind}
     */
    PropertyKind getPropertyKind();

    /**
     * Returns the property name.
     * @return the property name
     */
    String getName();

    /**
     * Represents a kind of {@link OperatorProperty}.
     */
    public enum PropertyKind {

        /**
         * operator input.
         */
        INPUT,

        /**
         * operator output.
         */
        OUTPUT,

        /**
         * operator value.
         */
        ARGUMENT,
    }
}
