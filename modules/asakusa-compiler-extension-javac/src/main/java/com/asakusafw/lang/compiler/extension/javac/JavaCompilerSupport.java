package com.asakusafw.lang.compiler.extension.javac;

import com.asakusafw.lang.compiler.common.DiagnosticException;

/**
 * Provides Java compiler features.
 */
public interface JavaCompilerSupport extends JavaSourceExtension {

    /**
     * Compiles previously {@link #addJavaFile(com.asakusafw.lang.compiler.model.description.ClassDescription) added}
     * Java source files.
     * @throws DiagnosticException if compilation was failed
     */
    void compile();
}
