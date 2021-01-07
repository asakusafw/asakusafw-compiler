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
package com.asakusafw.dag.runtime.jdbc.operation;

import java.util.function.Function;

/**
 * Provides contextual information of JDBC operations.
 * @since 0.4.0
 */
public interface JdbcContext {

    /**
     * Returns the environment.
     * @return the environment
     */
    JdbcEnvironment getEnvironment();

    /**
     * Resolves place-holders in the pattern string.
     * @param pattern the target pattern
     * @return the resolved string
     */
    String resolve(String pattern);

    /**
     * A basic implementation of {@link JdbcContext}.
     * @since 0.4.0
     */
    class Basic implements JdbcContext {

        private final JdbcEnvironment environment;

        private final Function<? super String, ? extends String> resolver;

        /**
         * Creates a new instance.
         * @param environment the current environment
         * @param resolver the variable resolver
         */
        public Basic(JdbcEnvironment environment, Function<? super String, ? extends String> resolver) {
            this.environment = environment;
            this.resolver = resolver;
        }

        @Override
        public JdbcEnvironment getEnvironment() {
            return environment;
        }

        @Override
        public String resolve(String pattern) {
            return resolver.apply(pattern);
        }
    }
}
