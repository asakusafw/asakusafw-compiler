package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a task in runtime.
 */
public interface TaskReference extends Reference {

    /**
     * Returns tasks which must be executed before this task.
     * @return the blocker tasks
     */
    Collection<? extends TaskReference> getBlockerTasks();

    /**
     * Phases each task replies on.
     */
    public enum Phase {

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
            if (symbol == null) {
                throw new IllegalArgumentException("symbol must not be null"); //$NON-NLS-1$
            }
            return Lazy.SYMBOLS.get(symbol);
        }

        @Override
        public String toString() {
            return getSymbol();
        }

        private static final class Lazy {

            static final Map<String, Phase> SYMBOLS;
            static {
                Map<String, Phase> map = new HashMap<String, Phase>();
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
