package com.asakusafw.lang.compiler.api;

/**
 * Provides API extensions.
 */
public interface ExtensionContainer {

    /**
     * Returns an extension service.
     * @param extension the extension type
     * @param <T> the extension type
     * @return the extension service, or {@code null} if it is not found
     */
    <T> T getExtension(Class<T> extension);
}
