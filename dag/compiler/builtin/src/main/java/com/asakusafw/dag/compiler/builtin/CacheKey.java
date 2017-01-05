/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.builtin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorArgument;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.model.graph.UserOperator;

/**
 * Represents a cache key for operator adapter class generators.
 * @since 0.4.0
 */
public final class CacheKey {

    private final Object[] elements;

    CacheKey(Object[] elements) {
        this.elements = elements;
    }

    /**
     * Creates a cache key for the target operator definition.
     * This only includes the operator method definition without applied any arguments.
     * @param operator the target operator
     * @return the created cache key
     */
    public static CacheKey of(CoreOperator operator) {
        return new Builder().operator(operator).build();
    }

    /**
     * Creates a cache key for the target operator definition.
     * This only includes the operator method definition without applied any arguments.
     * @param operator the target operator
     * @return the created cache key
     */
    public static CacheKey of(UserOperator operator) {
        return new Builder().operator(operator).build();
    }

    /**
     * Returns a new builder.
     * @return the created builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(elements);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CacheKey other = (CacheKey) obj;
        return Arrays.equals(elements, other.elements);
    }

    @Override
    public String toString() {
        return "Cache" + Arrays.toString(elements); //$NON-NLS-1$
    }

    /**
     * A builder for {@link CacheKey}.
     * @since 0.4.0
     */
    public static class Builder {

        private final List<Object> elements = new ArrayList<>();

        /**
         * Adds an operator definition and its I/O port types.
         * @param operator the target operator
         * @return this
         * @see #arguments(Operator)
         */
        public Builder operator(CoreOperator operator) {
            elements.add(operator.getCoreOperatorKind());
            operator.getInputs().stream().map(OperatorPort::getDataType).forEachOrdered(elements::add);
            operator.getOutputs().stream().map(OperatorPort::getDataType).forEachOrdered(elements::add);
            return this;
        }

        /**
         * Adds an operator definition and its I/O port types.
         * @param operator the target operator
         * @return this
         * @see #arguments(Operator)
         */
        public Builder operator(UserOperator operator) {
            elements.add(operator.getAnnotation());
            elements.add(operator.getMethod());
            elements.add(operator.getImplementationClass());
            operator.getInputs().stream().map(OperatorPort::getDataType).forEachOrdered(elements::add);
            operator.getOutputs().stream().map(OperatorPort::getDataType).forEachOrdered(elements::add);
            return this;
        }

        /**
         * Adds operator arguments.
         * @param operator the target operator
         * @return this
         */
        public Builder arguments(Operator operator) {
            operator.getArguments().stream().map(OperatorArgument::getValue).forEachOrdered(elements::add);
            return this;
        }

        /**
         * Adds a raw value.
         * @param value the value
         * @return this
         */
        public Builder raw(Object value) {
            elements.add(value);
            return this;
        }

        /**
         * Builds a {@link CacheKey}.
         * @return the created object
         */
        public CacheKey build() {
            return new CacheKey(elements.toArray());
        }
    }
}
