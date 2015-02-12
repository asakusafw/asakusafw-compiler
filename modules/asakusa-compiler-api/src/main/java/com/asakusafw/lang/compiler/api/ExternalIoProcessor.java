package com.asakusafw.lang.compiler.api;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Processes external I/Os.
 */
public interface ExternalIoProcessor {

    /**
     * Returns whether this processor can process the target input description.
     * @param context the current context
     * @param reference the target description
     * @return {@code true} if this processor can process the target description, otherwise {@code false}
     * @throws DiagnosticException if failed to extract information from the target description
     */
    boolean isSupported(Context context, ExternalInputReference reference);

    /**
     * Returns whether this processor can process the target output description.
     * @param context the current context
     * @param reference the target description
     * @return {@code true} if this processor can process the target description, otherwise {@code false}
     * @throws DiagnosticException if failed to extract information from the target description
     */
    boolean isSupported(Context context, ExternalOutputReference reference);

    /**
     * Validates external I/O descriptions and returns their diagnostics.
     * @param context the current context
     * @param inputs input descriptions
     * @param outputs output descriptions
     * @throws DiagnosticException if descriptions are something wrong
     */
    void validate(
            Context context,
            List<ExternalInputReference> inputs,
            List<ExternalOutputReference> outputs);

    /**
     * Processes external descriptions I/O and returns required tasks for target I/Os.
     * @param context the current context
     * @param inputs input descriptions
     * @param outputs output descriptions
     * @throws IOException if failed to generate task resources
     * @throws DiagnosticException if exception occurred while processing descriptions
     */
    void process(
            Context context,
            List<ExternalInputReference> inputs,
            List<ExternalOutputReference> outputs) throws IOException;

    /**
     * Represents a context object for {@link ExternalIoProcessor}.
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

        /**
         * Returns the data model loader.
         * @return the data model loader
         */
        DataModelLoader getDataModelLoader();

        /**
         * Adds a new Java class file and returns its output stream.
         * @param aClass the target class
         * @return the output stream to set the target file contents
         * @throws IOException if failed to create a new file
         * @see #addResourceFile(Location)
         */
        OutputStream addClassFile(ClassDescription aClass) throws IOException;

        /**
         * Adds a new classpath resource file and returns its output stream.
         * @param location the resource path (relative path from the classpath)
         * @return the output stream to set the target file contents
         * @throws IOException if failed to create a new file
         * @see #addClassFile(ClassDescription)
         */
        OutputStream addResourceFile(Location location) throws IOException;

        /**
         * Adds a sub-application to execute in this application.
         * @param phase the target execution phase
         * @param task the task reference
         */
        void addTask(TaskReference.Phase phase, TaskReference task);
    }
}
