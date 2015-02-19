package com.asakusafw.lang.compiler.core;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.common.BasicExtensionContainer;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.packaging.FileContainerRepository;

/**
 * An abstract interface of compiler context.
 */
public interface CompilerContext extends AnalyzerContext, ExtensionContainer {

    /**
     * Returns the compiler options.
     * @return the compiler options
     */
    CompilerOptions getOptions();

    /**
     * Returns the file container repository for temporary outputs.
     * @return the temporary outputs
     */
    FileContainerRepository getTemporaryOutputs();

    /**
     * A basic implementation of {@link CompilerContext}.
     * Clients can inherit this class.
     */
    public static class Basic extends BasicExtensionContainer implements CompilerContext {

        private final CompilerOptions options;

        private final ProjectRepository project;

        private final ToolRepository tools;

        private final FileContainerRepository temporary;

        /**
         * Creates a new instance.
         * @param options the compiler options
         * @param project the project information
         * @param tools the compiler tools
         * @param temporary the temporary output provider
         */
        public Basic(
                CompilerOptions options,
                ProjectRepository project,
                ToolRepository tools,
                FileContainerRepository temporary) {
            this.options = options;
            this.project = project;
            this.tools = tools;
            this.temporary = temporary;
        }

        @Override
        public CompilerOptions getOptions() {
            return options;
        }

        @Override
        public ProjectRepository getProject() {
            return project;
        }

        @Override
        public ToolRepository getTools() {
            return tools;
        }

        @Override
        public FileContainerRepository getTemporaryOutputs() {
            return temporary;
        }
    }
}