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
package com.asakusafw.lang.compiler.common;

/**
 * Utilities for {@link Predicate}.
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
        return new Predicate<T>() {
            @Override
            public boolean apply(T argument) {
                return true;
            }
        };
    }

    /**
     * Returns {@code FALSE} predicate.
     * @param <T> the target type
     * @return {@code FALSE} predicate
     */
    public static <T> Predicate<T> nothing() {
        return not(anything());
    }

    /**
     * Returns {@code NOT} predicate.
     * @param <T> the target type
     * @param p the predicate
     * @return {@code NOT} predicate
     */
    public static <T> Predicate<T> not(final Predicate<? super T> p) {
        return new Predicate<T>() {
            @Override
            public boolean apply(T argument) {
                return !p.apply(argument);
            }
        };
    }

    /**
     * Returns {@code AND} predicate.
     * @param <T> the target type
     * @param a the first predicate
     * @param b the second predicate
     * @return {@code AND} predicate
     */
    public static <T> Predicate<T> and(final Predicate<? super T> a, final Predicate<? super T> b) {
        return new Predicate<T>() {
            @Override
            public boolean apply(T argument) {
                return a.apply(argument) && b.apply(argument);
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
    public static <T> Predicate<T> or(final Predicate<? super T> a, final Predicate<? super T> b) {
        return new Predicate<T>() {
            @Override
            public boolean apply(T argument) {
                return a.apply(argument) || b.apply(argument);
            }
        };
    }
}
