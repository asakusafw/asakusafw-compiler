package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;

/**
 * Represents a core operator.
 */
public final class CoreOperator extends Operator {

    private final CoreOperatorKind kind;

    private CoreOperator(CoreOperatorKind kind) {
        this.kind = kind;
    }

    @Override
    public CoreOperator copy() {
        return copyAttributesTo(new CoreOperator(kind));
    }

    /**
     * Creates a new operator builder.
     * @param kind the core operator kind
     * @return the builder
     */
    public static Builder builder(CoreOperatorKind kind) {
        return new Builder(new CoreOperator(kind));
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.CORE;
    }

    /**
     * Returns the core operator kind of this.
     * @return the core operator kind
     */
    public CoreOperatorKind getCoreOperatorKind() {
        return kind;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "CoreOperator({0})", //$NON-NLS-1$
                kind);
    }

    /**
     * Represents a kind of core operator.
     */
    public static enum CoreOperatorKind {

        /**
         * Checkpoint operators.
         */
        CHECKPOINT,

        /**
         * Project operators.
         */
        PROJECT,

        /**
         * Extend operators.
         */
        EXTEND,

        /**
         * Restructure operators.
         */
        RESTRUCTURE,
    }

    /**
     * A builder for {@link CoreOperator}.
     */
    public static final class Builder extends AbstractBuilder<CoreOperator, Builder> {

        Builder(CoreOperator owner) {
            super(owner);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }
    }
}
