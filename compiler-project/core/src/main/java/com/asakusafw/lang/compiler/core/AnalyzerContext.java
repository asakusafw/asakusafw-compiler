/**
 * Copyright 2011-2017 Asakusa Framework Team.
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

/**
 * An abstract interface of analyzer context.
 */
public interface AnalyzerContext {

    /**
     * Returns the project information.
     * @return the project information
     */
    ProjectRepository getProject();

    /**
     * Returns the compiler tool repository.
     * @return the compiler tool repository
     */
    ToolRepository getTools();

    /**
     * A basic implementation of {@link AnalyzerContext}.
     * Clients can inherit this class.
     */
    class Basic implements AnalyzerContext {

        private final ProjectRepository project;

        private final ToolRepository tools;

        /**
         * Creates a new instance.
         * @param project the project repository
         * @param tools the tools repository
         */
        public Basic(ProjectRepository project, ToolRepository tools) {
            this.project = project;
            this.tools = tools;
        }

        @Override
        public ProjectRepository getProject() {
            return project;
        }

        @Override
        public ToolRepository getTools() {
            return tools;
        }
    }
}
