package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;

/**
 * Represents a reference with blockers.
 * @param <T> the blocker type
 */
public interface BlockingReference<T extends BlockingReference<T>> extends Reference {

    /**
     * Returns blocker references for this.
     * @return the blocker references
     */
    Collection<? extends T> getBlockers();
}
