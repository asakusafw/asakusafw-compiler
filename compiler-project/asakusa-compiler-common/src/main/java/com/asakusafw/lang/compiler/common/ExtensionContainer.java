package com.asakusafw.lang.compiler.common;

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

    /**
     * Editable {@link ExtensionContainer}.
     */
    public interface Editable extends ExtensionContainer {

        /**
         * Registers an extension service.
         * @param <T> extension type
         * @param extension extension type
         * @param service extension service, or {@code null} to remove the extension service
         */
        <T> void registerExtension(Class<T> extension, T service);
    }
}
