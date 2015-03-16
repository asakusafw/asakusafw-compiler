package com.asakusafw.lang.compiler.analyzer.mock;

/**
 * Accessor for non public description classes.
 */
public final class NotPublicAccessor {

    private NotPublicAccessor() {
        return;
    }

    /**
     * Returns the target class.
     * @return the target class
     */
    public static Class<NotPublicBatch> getBatch() {
        return NotPublicBatch.class;
    }

    /**
     * Returns the target class.
     * @return the target class
     */
    public static Class<NotPublicJobflow> getJobflow() {
        return NotPublicJobflow.class;
    }
}
