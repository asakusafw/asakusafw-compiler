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
package com.asakusafw.lang.compiler.core;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;

/**
 * Analyzes Asakusa DSL classes.
 */
public interface ClassAnalyzer {

    /**
     * Returns whether the target class represents a batch or not.
     * @param context the current context
     * @param aClass the target class
     * @return {@code true} if the target class represents a batch, otherwise {@code false}
     */
    boolean isBatchClass(Context context, Class<?> aClass);

    /**
     * Returns whether the target class represents a jobflow or not.
     * @param context the current context
     * @param aClass the target class
     * @return {@code true} if the target class represents a jobflow, otherwise {@code false}
     */
    boolean isJobflowClass(Context context, Class<?> aClass);

    /**
     * Returns whether the target object represents a flow or not.
     * @param context the current context
     * @param object the target object
     * @return {@code true} if the target class represents a flow, otherwise {@code false}
     * @since 0.3.0
     */
    boolean isFlowObject(Context context, Object object);

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
     * Analyzes a flow object.
     * @param context the current context
     * @param flowObject the target flow object
     * @return the analyzed element
     * @throws DiagnosticException if the target jobflow class is not valid
     * @see #isFlowObject(Context, Object)
     * @since 0.3.0
     */
    OperatorGraph analyzeFlow(Context context, Object flowObject);

    /**
     * A context for {@link ClassAnalyzer}.
     */
    class Context extends AnalyzerContext.Basic {

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
