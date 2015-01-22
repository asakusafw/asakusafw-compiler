package com.asakusafw.lang.compiler.api.basic;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.api.Diagnostic;

/**
 * A basic implementation of {@link Diagnostic}.
 */
public final class BasicDiagnostic implements Diagnostic {

    private static final long serialVersionUID = -3671335650818430741L;

    private Level level;

    private String message;

    private Exception cause;

    /**
     * Creates a new instance for serializer.
     */
    protected BasicDiagnostic() {
        return;
    }

    /**
     * Creates a new instance.
     * @param level the severity
     * @param message the diagnostic message
     */
    public BasicDiagnostic(Level level, String message) {
        this.level = level;
        this.message = message;
    }

    /**
     * Creates a new instance.
     * @param level the severity
     * @param message the diagnostic message
     * @param cause the causal exception
     */
    public BasicDiagnostic(Level level, String message, Exception cause) {
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
        return MessageFormat.format(
                "[{0}] {1}", //$NON-NLS-1$
                level,
                message);
    }
}