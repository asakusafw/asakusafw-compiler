/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
import java.io.InputStream;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.common.Location;

/**
 * Processes an Asakusa batch application.
 */
@FunctionalInterface
public interface BatchProcessor {

    /**
     * Processes the batch.
     * @param context the build context
     * @param source the target batch
     * @throws IOException if build was failed by I/O error
     * @throws DiagnosticException if build was failed with diagnostics
     */
    void process(Context context, BatchReference source) throws IOException;

    /**
     * Represents a context object for {@link BatchProcessor}.
     * @since 0.1.0
     * @version 0.3.1
     */
    public interface Context extends ExtensionContainer {

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
         * Adds a new resource file and returns its output stream.
         * @param location the resource path (relative path from the individual batch application root)
         * @return the output stream to set the target file contents
         * @throws IOException if failed to create a new file
         */
        OutputStream addResourceFile(Location location) throws IOException;

        /**
         * Returns a resource in a jobflow package.
         * @param jobflow the target jobflow reference
         * @param location the resource location in the target package
         * @return the jobflow resource, or {@code null} if it does not exist
         * @throws IOException if failed to open the file
         * @since 0.3.1
         */
        InputStream findResourceFile(JobflowReference jobflow, Location location) throws IOException;
    }
}
