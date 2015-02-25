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
