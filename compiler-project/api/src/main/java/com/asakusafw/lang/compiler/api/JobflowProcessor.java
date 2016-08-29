/**
 * Copyright 2011-2016 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.lang.compiler.api;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Processes operator graphs in jobflow.
 */
@FunctionalInterface
public interface JobflowProcessor {

    /**
     * Processes an operator graph in the jobflow.
     * @param context the build context
     * @param source the target jobflow
     * @throws IOException if build was failed by I/O error
     * @throws DiagnosticException if build was failed with diagnostics
     */
    void process(Context context, Jobflow source) throws IOException;

    /**
     * Represents a context object for {@link JobflowProcessor}.
     * @since 0.1.0
     * @version 0.3.0
     */
    public interface Context extends ExtensionContainer {

        /**
         * Returns the compiler options.
         * @return the compiler options
         */
        CompilerOptions getOptions();

        /**
         * Returns the target batch ID.
         * @return the target batch ID
         */
        String getBatchId();

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
         * <p>
         * Each path in the result must represent a URI (absolute or relative from the remote system working directory
         * on runtime), and may contain an asterisk ({@code '*'}) as the wildcard character.
         * If you want to use {@link CompilerOptions#getRuntimeWorkingDirectory() the framework specific remote
         * working directory}, you should insert the prefix manually.
         * </p>
         * <p>
         * If some sub-application independently processes the target external input,
         * you should not use this method.
         * </p>
         * @param name the input name (each input must be unique in the jobflow)
         * @param info the structural information of the target external input
         * @return the resolved symbol
         */
        ExternalInputReference addExternalInput(
                String name,
                ExternalInputInfo info);

        /**
         * Adds an external output operator in this application.
         * <p>
         * Each path must represent a URI (absolute or relative from the remote system working directory on runtime),
         * and can contain an asterisk ({@code '*'}) as the wildcard character.
         * If you want to use {@link CompilerOptions#getRuntimeWorkingDirectory() the framework specific remote
         * working directory}, you should insert the prefix manually.
         * </p>
         * <p>
         * If some sub-application independently processes the target external output,
         * you <em>MUST NOT</em> use this method.
         * </p>
         * @param name the output name (each output must be unique in the jobflow)
         * @param info the structural information of the target external output
         * @param internalOutputPaths the output paths which will be internally generated in this jobflow
         * @return the resolved symbol
         */
        ExternalOutputReference addExternalOutput(
                String name,
                ExternalOutputInfo info,
                Collection<String> internalOutputPaths);

        /**
         * Adds a sub-application to execute in this application.
         * @param moduleName the target module name only consists of lower-letters and digits
         *    (like as {@code "windgate"}, {@code "spark"}, etc.)
         * @param profileName the profile name where the command is running on
         * @param command command path (relative from {@code ASAKUSA_HOME})
         * @param arguments command arguments
         * @param blockers the blocker sub-applications
         * @return a symbol that represents the added sub-application
         */
        TaskReference addTask(
                String moduleName,
                String profileName,
                Location command,
                List<? extends CommandToken> arguments,
                TaskReference... blockers);

        /**
         * Adds a sub-application to execute in this application.
         * @param moduleName the target module name only consists of lower-letters and digits
         *    (like as {@code "windgate"}, {@code "spark"}, etc.)
         * @param profileName the profile name where the command is running on
         * @param command command path (relative from {@code ASAKUSA_HOME})
         * @param arguments command arguments
         * @param extensions the acceptable extension names
         * @param blockers the blocker sub-applications
         * @return a symbol that represents the added sub-application
         * @since 0.3.0
         */
        TaskReference addTask(
                String moduleName,
                String profileName,
                Location command,
                List<? extends CommandToken> arguments,
                Collection<String> extensions,
                TaskReference... blockers);

        /**
         * Adds a sub-application to execute in finalize phase.
         * @param moduleName the target module name only consists of lower-letters and digits
         *    (like as {@code "windgate"}, {@code "spark"}, etc.)
         * @param profileName the profile name where the command is running on
         * @param command command path (relative from {@code ASAKUSA_HOME})
         * @param arguments command arguments
         * @param blockers the blocker sub-applications
         * @return a symbol that represents the added sub-application
         */
        TaskReference addFinalizer(
                String moduleName,
                String profileName,
                Location command,
                List<? extends CommandToken> arguments,
                TaskReference... blockers);

        /**
         * Adds a sub-application to execute in finalize phase.
         * @param moduleName the target module name only consists of lower-letters and digits
         *    (like as {@code "windgate"}, {@code "spark"}, etc.)
         * @param profileName the profile name where the command is running on
         * @param command command path (relative from {@code ASAKUSA_HOME})
         * @param arguments command arguments
         * @param extensions the acceptable extension names
         * @param blockers the blocker sub-applications
         * @return a symbol that represents the added sub-application
         * @since 0.3.0
         */
        TaskReference addFinalizer(
                String moduleName,
                String profileName,
                Location command,
                List<? extends CommandToken> arguments,
                Collection<String> extensions,
                TaskReference... blockers);
    }
}
