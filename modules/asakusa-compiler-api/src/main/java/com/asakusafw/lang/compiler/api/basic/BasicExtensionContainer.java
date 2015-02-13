package com.asakusafw.lang.compiler.api.basic;

import java.util.HashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.api.ExtensionContainer;

/**
 * A basic implementation of {@link ExtensionContainer}.
 */
public class BasicExtensionContainer implements ExtensionContainer {

    private final Map<Class<?>, Object> extensions = new HashMap<>();

    /**
     * Registers an extension service.
     * @param <T> extension type
     * @param extension extension type
     * @param service extension service, or {@code null} to remove the extension service
     */
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
