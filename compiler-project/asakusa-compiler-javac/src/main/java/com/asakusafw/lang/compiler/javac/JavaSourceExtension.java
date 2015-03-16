package com.asakusafw.lang.compiler.javac;

import java.io.IOException;
import java.io.Writer;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An extension for using Java source files.
 */
public interface JavaSourceExtension {

    /**
     * Adds a new Java source file and returns its output stream.
     * @param aClass the target class
     * @return the output stream to set the target file contents
     * @throws IOException if failed to create a new file
     */
    Writer addJavaFile(ClassDescription aClass) throws IOException;
}
