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
package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a task in runtime.
 * @since 0.3.0
 */
public interface TaskReference extends BlockingReference<TaskReference> {

    /**
     * Returns the module name for processing this task.
     * The module name must consist of lower-case alphabets or digits.
     * @return the module name
     */
    String getModuleName();

    /**
     * Returns the acceptable extension names for this task.
     * @return the acceptable extension names
     * @since 0.3.0
     */
    Set<String> getExtensions();

    /**
     * Returns tasks which must be executed before this task.
     * @return the blocker tasks
     */
    @Override
    Collection<? extends TaskReference> getBlockers();

    /**
     * Phases each task replies on.
     */
    enum Phase {

        /**
         * Initialization.
         */
        INITIALIZE,

        /**
         * Importing input data.
         */
        IMPORT,

        /**
         * Pre-processing input data.
         */
        PROLOGUE,

        /**
         * Processing data.
         */
        MAIN,

        /**
         * Post-processing output data.
         */
        EPILOGUE,

        /**
         * Exporting output data.
         */
        EXPORT,

        /**
         * Finalization.
         */
        FINALIZE,
        ;

        /**
         * Returns the symbol of this phase.
         * @return the symbol of this phase
         */
        public String getSymbol() {
            return name().toLowerCase();
        }

        /**
         * Returns an {@link Phase} corresponded to the symbol.
         * @param symbol target symbol
         * @return the corresponding phase, or {@code null} if not found
         * @throws IllegalArgumentException if some parameters were {@code null}
         */
        public static Phase findFromSymbol(String symbol) {
            Objects.requireNonNull(symbol, "symbol must not be null"); //$NON-NLS-1$
            return Lazy.SYMBOLS.get(symbol);
        }

        @Override
        public String toString() {
            return getSymbol();
        }

        private static final class Lazy {

            static final Map<String, Phase> SYMBOLS;
            static {
                Map<String, Phase> map = new HashMap<>();
                for (Phase phase : values()) {
                    map.put(phase.getSymbol(), phase);
                }
                SYMBOLS = Collections.unmodifiableMap(map);
            }

            private Lazy() {
                return;
            }
        }
    }
}
