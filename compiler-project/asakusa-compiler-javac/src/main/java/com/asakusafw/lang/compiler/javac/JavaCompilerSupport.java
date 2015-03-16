package com.asakusafw.lang.compiler.javac;

import com.asakusafw.lang.compiler.common.DiagnosticException;

/**
 * Provides Java compiler features.
 */
public interface JavaCompilerSupport extends JavaSourceExtension {

    /**
     * Performs compiling Java source files.
     * @throws DiagnosticException if compilation was failed
     */
    void process();
}
