package com.asakusafw.lang.compiler.core.util;

import java.util.Collection;

/**
 * An abstract super interface of composition objects.
 * @param <E> the element type
 */
public interface CompositeElement<E> {

    /**
     * Returns the elements in this composition object.
     * @return the elements
     */
    Collection<? extends E> getElements();
}
