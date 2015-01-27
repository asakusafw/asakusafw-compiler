package com.asakusafw.lang.compiler.planning;

import java.util.Collection;

/**
 * An abstract super interface which provides free-formed attributes.
 */
public interface AttributeContainer {

    /**
     * Returns a registered attribute.
     * @param type the attribute type
     * @param <T> the attribute type
     * @return the corresponded attribute, or {@code null} if such an attribute is not registered
     */
    <T> T getAttribute(Class<T> type);

    /**
     * Registers an attribute.
     * @param type the attribute type
     * @param value the attribute value, or {@code null} to remove the registered attribute
     * @param <T> the attribute type
     */
    <T> void putAttribute(Class<T> type, T value);

    /**
     * Returns the registered attribute types.
     * @return the registered attribute types
     */
    Collection<Class<?>> getAttributeTypes();
}
