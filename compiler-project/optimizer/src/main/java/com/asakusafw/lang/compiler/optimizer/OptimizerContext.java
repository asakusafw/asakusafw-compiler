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
package com.asakusafw.lang.compiler.optimizer;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Context for optimizers.
 * @since 0.1.0
 * @version 0.3.1
 */
public interface OptimizerContext extends ExtensionContainer {

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
     * Returns the target flow ID.
     * @return the target flow ID
     */
    String getFlowId();

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
     * Returns the optimizer toolkit.
     * @return the optimizer toolkit
     * @since 0.3.1
     */
    OptimizerToolkit getToolkit();

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
}
