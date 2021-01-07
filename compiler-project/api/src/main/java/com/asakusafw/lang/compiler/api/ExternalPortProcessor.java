/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
import java.util.Map;

import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Processes external I/O ports.
 */
public interface ExternalPortProcessor {

    /**
     * Returns whether this processor can process the target input/output description.
     * @param context the current context
     * @param descriptionClass the target external input/output description type
     * @return {@code true} if this processor can process the target description type, otherwise {@code false}
     * @throws DiagnosticException if failed to extract information from the target description
     */
    boolean isSupported(AnalyzeContext context, Class<?> descriptionClass);

    /**
     * Analyzes external input description and returns structural information of it.
     * @param context the current context
     * @param name the target input name
     * @param description the target description object
     * @return the analyzed information
     * @throws DiagnosticException if failed to resolve the target description
     */
    ExternalInputInfo analyzeInput(AnalyzeContext context, String name, Object description);

    /**
     * Analyzes external output description and returns structural information of it.
     * @param context the current context
     * @param name the target output name
     * @param description the target description object
     * @return the analyzed information
     * @throws DiagnosticException if failed to resolve the target description
     */
    ExternalOutputInfo analyzeOutput(AnalyzeContext context, String name, Object description);

    /**
     * Validates external inputs and outputs.
     * @param context the current context
     * @param inputs the external inputs
     * @param outputs the external outputs
     * @throws DiagnosticException if failed to resolve the target description
     */
    void validate(
            AnalyzeContext context,
            Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs);

    /**
     * Resolves external input and returns a reference to the input port.
     * @param context the current context
     * @param name the target output name
     * @param info the structural information of the target external input
     * @return the resolved reference
     */
    ExternalInputReference resolveInput(Context context, String name, ExternalInputInfo info);

    /**
     * Resolves external output and returns a reference to the output port.
     * @param context the current context
     * @param name the target output name
     * @param info the structural information of the target external output
     * @param internalOutputPaths the output file paths which have been generated before
     *     {@link com.asakusafw.lang.compiler.api.reference.TaskReference.Phase#EPILOGUE epilogue phase}
     * @return the resolved reference
     */
    ExternalOutputReference resolveOutput(
            Context context,
            String name, ExternalOutputInfo info,
            Collection<String> internalOutputPaths);

    /**
     * Processes external descriptions I/O.
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
     * Returns the adapter object for processing the target description type.
     * @param <T> the adapter type
     * @param context the current context
     * @param adapterType the adapter type
     * @param descriptionClass the target external input/output description type
     * @return the corresponded adapter object, or {@code null} if this does not support the target adapter type
     */
    <T> T getAdaper(AnalyzeContext context, Class<T> adapterType, Class<?> descriptionClass);

    /**
     * Represents a context object for {@link ExternalPortProcessor} only for analyzing DSL descriptions.
     */
    public interface AnalyzeContext {

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
    }

    /**
     * Represents a context object for {@link ExternalPortProcessor}.
     */
    public interface Context extends AnalyzeContext, ExtensionContainer {

        /**
         * Returns the compiler options.
         * @return the compiler options
         */
        CompilerOptions getOptions();

        /**
         * Returns the current batch ID.
         * @return the current batch ID
         */
        String getBatchId();

        /**
         * Returns the current flow ID.
         * @return the current flow ID
         */
        String getFlowId();

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
