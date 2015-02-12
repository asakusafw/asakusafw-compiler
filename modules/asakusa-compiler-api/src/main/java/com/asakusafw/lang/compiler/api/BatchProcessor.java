package com.asakusafw.lang.compiler.api;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.model.Location;

/**
 * Processes an Asakusa batch application.
 */
public interface BatchProcessor {

    /**
     * Processes the batch.
     * @param context the build context
     * @param source the target batch
     * @throws IOException if build was failed by I/O error
     * @throws DiagnosticException if build was failed with diagnostics
     */
    void process(Context context, BatchReference source) throws IOException, DiagnosticException;

    /**
     * Represents a context object for {@link BatchProcessor}.
     */
    public static interface Context extends ExtensionContainer {

        /**
         * Returns the compiler options.
         * @return the compiler options
         */
        CompilerOptions getOptions();

        /**
         * Adds a new resource file and returns its output stream.
         * @param location the resource path (relative path from the individual batch application root)
         * @return the output stream to set the target file contents
         * @throws IOException if failed to create a new file
         */
        OutputStream addResourceFile(Location location) throws IOException;
    }
}
