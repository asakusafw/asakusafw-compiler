/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.common;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Utilities for {@link Predicate}.
 * @since 0.1.0
 * @version 0.4.0
 */
public final class Predicates {

    private Predicates() {
        return;
    }

    /**
     * Returns {@code TRUE} predicate.
     * @param <T> the target type
     * @return {@code TRUE} predicate
     */
    public static <T> Predicate<T> anything() {
        return argument -> true;
    }

    /**
     * Returns {@code FALSE} predicate.
     * @param <T> the target type
     * @return {@code FALSE} predicate
     */
    public static <T> Predicate<T> nothing() {
        return arguments -> false;
    }

    /**
     * Returns {@code NOT} predicate.
     * @param <T> the target type
     * @param p the predicate
     * @return {@code NOT} predicate
     */
    public static <T> Predicate<T> not(Predicate<? super T> p) {
        return argument -> !p.test(argument);
    }

    /**
     * Returns {@code AND} predicate.
     * @param <T> the target type
     * @param a the first predicate
     * @param b the second predicate
     * @return {@code AND} predicate
     */
    public static <T> Predicate<T> and(Predicate<? super T> a, Predicate<? super T> b) {
        return new Composite<T>("All", a, b) { //$NON-NLS-1$
            @Override
            public boolean test(T argument) {
                for (Predicate<? super T> p : elements) {
                    if (p.test(argument) == false) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    /**
     * Returns {@code OR} predicate.
     * @param <T> the target type
     * @param a the first predicate
     * @param b the second predicate
     * @return {@code OR} predicate
     */
    public static <T> Predicate<T> or(Predicate<? super T> a, Predicate<? super T> b) {
        return new Composite<T>("Exists", a, b) { //$NON-NLS-1$
            @Override
            public boolean test(T argument) {
                for (Predicate<? super T> p : elements) {
                    if (p.test(argument)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    private abstract static class Composite<T> implements Predicate<T> {

        private final String operator;

        final List<Predicate<? super T>> elements = new ArrayList<>();

        Composite(String operator, Predicate<? super T> a, Predicate<? super T> b) {
            this.operator = operator;
            add(a);
            add(b);
        }

        private void add(Predicate<? super T> p) {
            if (p.getClass() == getClass()) {
                elements.addAll(((Composite<? super T>) p).elements);
            } else {
                elements.add(p);
            }
        }

        @Override
        public String toString() {
            return operator + elements;
        }
    }
}
