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
        CHECKPOINT("Checkpoint"), //$NON-NLS-1$

        /**
         * Project operators.
         */
        PROJECT("Project"), //$NON-NLS-1$

        /**
         * Extend operators.
         */
        EXTEND("Extend"), //$NON-NLS-1$

        /**
         * Restructure operators.
         */
        RESTRUCTURE("Restructure"), //$NON-NLS-1$
        ;

        private final String symbol;

        private CoreOperatorKind(String symbol) {
            this.symbol = symbol;
        }

        @Override
        public String toString() {
            return symbol;
        }
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
