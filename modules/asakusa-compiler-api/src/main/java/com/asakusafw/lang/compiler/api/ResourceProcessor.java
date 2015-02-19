package com.asakusafw.lang.compiler.api;

import java.io.IOException;

import com.asakusafw.lang.compiler.common.DiagnosticException;

/**
 * Processes resources generated in compilation.
 */
public interface ResourceProcessor extends ResourceContainer {

    /**
     * Processes the previously added resources.
     * @param context the current context
     * @throws IOException if failed to process resources by I/O error
     * @throws DiagnosticException if exception occurred while processing resources
     */
    void process(Context context) throws IOException;

    /**
     * Represents a context object for {@link ResourceProcessor}.
     */
    public static interface Context extends ExtensionContainer {

        /**
         * Returns the compiler options.
         * @return the compiler options
         */
        CompilerOptions getOptions();

        /**
         * Returns the class loader to obtain the target application classes.
         * @return the class loader
         */
        ClassLoader getClassLoader();
    }
}
