package com.asakusafw.lang.compiler.core;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;

/**
 * Analyzes Asakusa DSL classes.
 */
public interface ClassAnalyzer {

    /**
     * Analyzes batch class.
     * @param context the current context
     * @param batchClass the target batch class
     * @return the analyzed element
     * @throws DiagnosticException if the target batch class is not valid
     */
    Batch analyzeBatch(Context context, Class<?> batchClass);

    /**
     * Analyzes jobflow class.
     * @param context the current context
     * @param jobflowClass the target jobflow class
     * @return the analyzed element
     * @throws DiagnosticException if the target jobflow class is not valid
     */
    Jobflow analyzeJobflow(Context context, Class<?> jobflowClass);

    /**
     * A context for {@link ClassAnalyzer}.
     */
    public static class Context extends AnalyzerContext.Basic {

        /**
         * Creates a new instance.
         * @param parent the parent context
         */
        public Context(AnalyzerContext parent) {
            this(parent.getProject(), parent.getTools());
        }

        /**
         * Creates a new instance.
         * @param project the project information
         * @param tools the compiler tools
         */
        public Context(ProjectRepository project, ToolRepository tools) {
            super(project, tools);
        }
    }
}
