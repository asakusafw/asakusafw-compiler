package com.asakusafw.lang.tool.launcher;

/**
 * Represents an error while launching applications.
 */
public class LauncherException extends RuntimeException {

    private static final long serialVersionUID = -5566847104874631897L;

    /**
     * Creates a new instance.
     */
    public LauncherException() {
        super();
    }

    /**
     * Creates a new instance.
     * @param message the message
     */
    public LauncherException(String message) {
        super(message);
    }

    /**
     * Creates a new instance.
     * @param message the message
     * @param cause the cause of this exception
     */
    public LauncherException(String message, Throwable cause) {
        super(message, cause);
    }
}
