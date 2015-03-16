package com.asakusafw.lang.compiler.packaging;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Provides binary contents.
 */
public interface ContentProvider {

    /**
     * Provides binary contents to the target stream.
     * The {@link OutputStream} will be closed after this method execution was finished,
     * so clients may or may not close it.
     * @param output the file contents output
     * @throws IOException if error occurred while executing this method
     */
    void writeTo(OutputStream output) throws IOException;
}