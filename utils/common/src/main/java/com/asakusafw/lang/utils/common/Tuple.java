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
package com.asakusafw.lang.utils.common;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a tuple.
 * @param <TLeft> the left value type
 * @param <TRight> the right value type
 * @since 0.4.0
 */
public class Tuple<TLeft, TRight> {

    private final TLeft left;

    private final TRight right;

    /**
     * Creates a new instance.
     * @param left the left value
     * @param right the right value
     */
    public Tuple(TLeft left, TRight right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Creates a new instance from map entry.
     * @param entry the map entry
     * @param <TLeft> the left value type
     * @param <TRight> the right value type
     * @return the created instance
     */
    public static <TLeft, TRight> Tuple<TLeft, TRight> of(Map.Entry<? extends TLeft, ? extends TRight> entry) {
        Arguments.requireNonNull(entry);
        return new Tuple<>(entry.getKey(), entry.getValue());
    }

    /**
     * Returns the left value.
     * @return the left value
     */
    public TLeft left() {
        return left;
    }

    /**
     * Returns the right value.
     * @return the right value
     */
    public TRight right() {
        return right;
    }

    /**
     * Returns the left value.
     * @return the left value
     */
    public TLeft getLeft() {
        return left;
    }

    /**
     * Returns the right value.
     * @return the right value
     */
    public TRight getRight() {
        return right;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Objects.hashCode(left);
        result = prime * result + Objects.hashCode(right);
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
        Tuple<?, ?> other = (Tuple<?, ?>) obj;
        if (!Objects.equals(left, other.left)) {
            return false;
        }
        if (!Objects.equals(right, other.right)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.format("<%s, %s>", left, right); //$NON-NLS-1$
    }
}
