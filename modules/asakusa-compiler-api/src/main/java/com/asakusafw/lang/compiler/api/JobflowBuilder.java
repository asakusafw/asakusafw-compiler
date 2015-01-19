package com.asakusafw.lang.compiler.api;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.ExternalPortReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;

/**
 * Builds an Asakusa application from jobflow.
 */
public interface JobflowBuilder {

    /**
     * Builds the application.
     * @param context the build context
     * @param flow the target jobflow
     * @throws IOException if build was failed by I/O error
     * @throws DiagnosticException if build was failed with diagnostics
     */
    void process(Context context, Jobflow flow) throws IOException, DiagnosticException;

    /**
     * Represents a context object for {@link JobflowBuilder}.
     */
    public static interface Context {

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
         * Adds an external input operator in this application.
         * If some sub-application independently processes the target external input,
         * you should not use this method.
         * @param port the target input
         * @return the resolved symbol
         */
        ExternalPortReference<ExternalInput> addExternalInput(ExternalInput port);

        /**
         * Adds an external output operator in this application.
         * If some sub-application independently processes the target external output,
         * you <em>MUST NOT</em> use this method.
         * @param port the target output
         * @return the resolved symbol
         */
        ExternalPortReference<ExternalOutput> addExternalOutput(ExternalOutput port);

        /**
         * Adds a sub-application to execute in this application.
         * @param profileName the profile name where the command is running on
         * @param command command path (relative from {@code ASAKUSA_HOME})
         * @param arguments command arguments
         * @param blockers the blocker sub-applications
         * @return a symbol that represents the added sub-application
         */
        TaskReference addTask(
                String profileName,
                Location command,
                List<? extends CommandToken> arguments,
                TaskReference... blockers);

        /**
         * Adds a sub-application to execute in finalizer.
         * @param profileName the profile name where the command is running on
         * @param command command path (relative from {@code ASAKUSA_HOME})
         * @param arguments command arguments
         * @param blockers the blocker sub-applications
         * @return a symbol that represents the added sub-application
         */
        TaskReference addFinalizer(
                String profileName,
                Location command,
                List<? extends CommandToken> arguments,
                TaskReference... blockers);
    }
}
