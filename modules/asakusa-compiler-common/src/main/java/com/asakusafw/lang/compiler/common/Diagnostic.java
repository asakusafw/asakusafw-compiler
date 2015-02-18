package com.asakusafw.lang.compiler.common;

import java.io.Serializable;

/**
 * Represents a diagnostic information.
 */
public interface Diagnostic extends Serializable {

    /**
     * Returns the severity of this.
     * @return the severity
     */
    Level getLevel();

    /**
     * Returns the diagnostic message.
     * @return the diagnostic message
     */
    String getMessage();

    /**
     * Returns the optional causal information.
     * @return the causal exception, or {@code null} if it is not defined
     */
    Exception getException();

    /**
     * Represents severity of {@link Diagnostic}.
     */
    public static enum Level {

        /**
         * Information.
         */
        INFO,

        /**
         * Warning.
         */
        WARN,

        /**
         * Error.
         */
        ERROR,
    }
}
