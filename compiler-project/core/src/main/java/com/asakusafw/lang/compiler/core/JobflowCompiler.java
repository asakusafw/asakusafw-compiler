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
package com.asakusafw.lang.compiler.core;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.basic.ExternalPortContainer;
import com.asakusafw.lang.compiler.api.basic.TaskContainerMap;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.lang.compiler.packaging.FileContainerRepository;

/**
 * Compiles Asakusa jobflows.
 */
@FunctionalInterface
public interface JobflowCompiler {

    /**
     * Compiles the target jobflow.
     * @param context the current context
     * @param batch information of the jobflow owner
     * @param jobflow the target jobflow
     * @throws DiagnosticException if compilation was failed
     */
    void compile(Context context, BatchInfo batch, Jobflow jobflow);

    /**
     * A context for {@link JobflowCompiler}.
     */
    class Context extends CompilerContext.Basic {

        private final FileContainer output;

        private final TaskContainerMap taskContainerMap = new TaskContainerMap();

        private final ExternalPortContainer externalPorts = new ExternalPortContainer();

        /**
         * Creates a new instance from parent compiler context.
         * Note that, this does NOT inherit the extension objects from the parent context.
         * @param parent the parent context
         * @param output the package element output
         */
        public Context(CompilerContext parent, FileContainer output) {
            this(parent.getOptions(), parent.getProject(), parent.getTools(), output, parent.getTemporaryOutputs());
        }

        /**
         * Creates a new instance.
         * @param options the compiler options
         * @param project the project information
         * @param tools the compiler tools
         * @param output the package elements output
         * @param temporary the temporary output provider
         */
        public Context(
                CompilerOptions options,
                ProjectRepository project,
                ToolRepository tools,
                FileContainer output,
                FileContainerRepository temporary) {
            super(options, project, tools, temporary);
            this.output = output;
        }

        /**
         * Returns the file container for jobflow package output.
         * @return the output file container
         */
        public FileContainer getOutput() {
            return output;
        }

        /**
         * Returns the task container map for storing jobflow tasks.
         * @return the task container map
         */
        public TaskContainerMap getTaskContainerMap() {
            return taskContainerMap;
        }

        /**
         * Returns the external ports container for the current jobflow.
         * @return the external ports container
         */
        public ExternalPortContainer getExternalPorts() {
            return externalPorts;
        }
    }
}
