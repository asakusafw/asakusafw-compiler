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
package com.asakusafw.dag.compiler.flow;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.planning.Plan;

/**
 * Provides {@link ExternalPortDriver}.
 * @since 0.4.0
 */
public interface ExternalPortDriverProvider {

    /**
     * Creates a new instance.
     * @param context the current context
     * @return the created instance
     */
    ExternalPortDriver newInstance(Context context);

    /**
     * Represents a context of {@link ExternalPortDriverProvider}.
     * @since 0.4.0
     */
    class Context {

        private final CompilerOptions options;

        private final ClassGeneratorContext generatorContext;

        private final DagDescriptorFactory descriptors;

        private final Plan sourcePlan;

        public Context(
                CompilerOptions options,
                ClassGeneratorContext generatorContext,
                DagDescriptorFactory descriptors,
                Plan sourcePlan) {
            this.options = options;
            this.generatorContext = generatorContext;
            this.descriptors = descriptors;
            this.sourcePlan = sourcePlan;
        }

        /**
         * Returns the current compiler options.
         * @return the current compiler options
         */
        public CompilerOptions getOptions() {
            return options;
        }

        /**
         * Returns the current class generator context.
         * @return the current class generator context
         */
        public ClassGeneratorContext getGeneratorContext() {
            return generatorContext;
        }

        /**
         * Returns the descriptor factory.
         * @return the descriptor factory
         */
        public DagDescriptorFactory getDescriptorFactory() {
            return descriptors;
        }

        /**
         * Returns the source plan.
         * @return the source plan
         */
        public Plan getSourcePlan() {
            return sourcePlan;
        }
    }
}
