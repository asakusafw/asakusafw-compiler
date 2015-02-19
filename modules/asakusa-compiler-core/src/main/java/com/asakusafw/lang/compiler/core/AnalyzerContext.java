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
    public static class Basic implements AnalyzerContext {

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
