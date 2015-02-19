package com.asakusafw.lang.compiler.common;

import java.util.HashMap;
import java.util.Map;

/**
 * A basic implementation of {@link ExtensionContainer}.
 */
public class BasicExtensionContainer implements ExtensionContainer.Editable {

    private final Map<Class<?>, Object> extensions = new HashMap<>();

    @Override
    public <T> void registerExtension(Class<T> extension, T service) {
        if (service == null) {
            extensions.remove(extension);
        } else {
            extensions.put(extension, service);
        }
    }

    @Override
    public <T> T getExtension(Class<T> extension) {
        Object service = extensions.get(extension);
        if (service == null) {
            return null;
        }
        return extension.cast(service);
    }
}
