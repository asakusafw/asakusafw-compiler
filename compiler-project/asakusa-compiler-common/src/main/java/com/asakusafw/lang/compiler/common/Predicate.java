package com.asakusafw.lang.compiler.common;

/**
 * Represents a predicate.
 * @param <T> member type
 */
public interface Predicate<T> {

    /**
     * Returns whether the argument satisfies this predicate or not.
     * @param argument the target argument
     * @return {@code true} if the argument satisfies this, otherwise {@code false}
     */
    boolean apply(T argument);
}
