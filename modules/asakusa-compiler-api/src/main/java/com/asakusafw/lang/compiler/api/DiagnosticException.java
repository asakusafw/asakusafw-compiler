package com.asakusafw.lang.compiler.api;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents an exception with {@link Diagnostic}s.
 */
public class DiagnosticException extends Exception {

    private static final long serialVersionUID = 1L;

    private final Diagnostic[] diagnostics;

    /**
     * Creates a new instance for serializer.
     */
    protected DiagnosticException() {
        this.diagnostics = null;
    }

    /**
     * Creates a new instance.
     * @param severity the severity
     * @param message the message
     */
    public DiagnosticException(Diagnostic.Level severity, String message) {
        this(new SimpleDiagnostic(severity, message, null));
    }

    /**
     * Creates a new instance.
     * @param severity the severity
     * @param message the message
     * @param cause the original cause
     */
    public DiagnosticException(Diagnostic.Level severity, String message, Exception cause) {
        this(new SimpleDiagnostic(severity, message, cause));
    }

    /**
     * Creates a new instance.
     * @param diagnostics the diagnostics
     */
    public DiagnosticException(Diagnostic... diagnostics) {
        this(Arrays.asList(diagnostics));
    }

    /**
     * Creates a new instance.
     * @param diagnostics diagnostics
     */
    public DiagnosticException(Collection<? extends Diagnostic> diagnostics) {
        super(extractMessage(diagnostics), extractCause(diagnostics));
        this.diagnostics = diagnostics.toArray(new Diagnostic[diagnostics.size()]);
    }

    /**
     * Returns the diagnostics of this exception.
     * @return the diagnostics
     */
    public List<Diagnostic> getDiagnostics() {
        return Collections.unmodifiableList(Arrays.asList(diagnostics));
    }

    private static String extractMessage(Collection<? extends Diagnostic> diagnostics) {
        Diagnostic diagnostic = first(diagnostics);
        if (diagnostic == null) {
            return null;
        }
        if (diagnostics.size() == 1) {
            return diagnostic.getMessage();
        } else {
            return MessageFormat.format(
                    "{0}, ...and {1} more",
                    diagnostic.getMessage(),
                    diagnostics.size() - 1);
        }
    }

    private static Throwable extractCause(Collection<? extends Diagnostic> diagnostics) {
        Diagnostic diagnostic = first(diagnostics);
        if (diagnostic == null) {
            return null;
        }
        return diagnostic.getException();
    }

    private static Diagnostic first(Collection<? extends Diagnostic> diagnostics) {
        for (Diagnostic.Level level : new Diagnostic.Level[] { Diagnostic.Level.ERROR, Diagnostic.Level.WARN, }) {
            for (Diagnostic diagnostic : diagnostics) {
                if (diagnostic.getLevel() == level) {
                    return diagnostic;
                }
            }
        }
        return null;
    }

    /**
     * A simple implementation of {@link Diagnostic}.
     */
    public static final class SimpleDiagnostic implements Diagnostic {

        private static final long serialVersionUID = 2675222764030731891L;

        private Level level;

        private String message;

        private Exception cause;

        /**
         * Creates a new instance for serializer.
         */
        protected SimpleDiagnostic() {
            return;
        }

        /**
         * Creates a new instance.
         * @param level the severity
         * @param message the diagnostic message
         */
        public SimpleDiagnostic(Level level, String message) {
            this.level = level;
            this.message = message;
        }

        /**
         * Creates a new instance.
         * @param level the severity
         * @param message the diagnostic message
         * @param cause the causal exception
         */
        public SimpleDiagnostic(Level level, String message, Exception cause) {
            this.level = level;
            this.message = message;
            this.cause = cause;
        }

        @Override
        public Level getLevel() {
            return level;
        }

        @Override
        public String getMessage() {
            return message;
        }

        @Override
        public Exception getException() {
            return cause;
        }

        @Override
        public String toString() {
            return String.format("[%s] %s", getLevel(), getMessage()); //$NON-NLS-1$
        }
    }
}
