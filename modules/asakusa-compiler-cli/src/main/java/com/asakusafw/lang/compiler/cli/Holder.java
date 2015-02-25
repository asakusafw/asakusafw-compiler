package com.asakusafw.lang.compiler.cli;

/**
 * Represents a value or values container.
 * @param <T> the value type
 */
public interface Holder<T> extends Iterable<T> {

    /**
     * Returns whether this holder is empty or not.
     * @return {@code true} if this holder is empty, otherwise {@code false}
     */
    boolean isEmpty();
}
