package com.asakusafw.lang.compiler.core;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.lang.compiler.packaging.FileContainerRepository;

/**
 * Compiles Asakusa batches.
 */
public interface BatchCompiler {

    /**
     * Compiles the target jobflow.
     * @param context the current context
     * @param batch the target batch
     * @throws DiagnosticException if compilation was failed
     */
    void compile(Context context, Batch batch);

    /**
     * A context for {@link BatchCompiler}.
     */
    public static class Context extends CompilerContext.Basic {

        private final FileContainer output;

        /**
         * Creates a new instance.
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
         * Returns the file container for batch package output.
         * @return the output file container
         */
        public FileContainer getOutput() {
            return output;
        }
    }
}
