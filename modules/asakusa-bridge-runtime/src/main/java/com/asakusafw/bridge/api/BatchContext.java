package com.asakusafw.bridge.api;

import java.text.MessageFormat;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.stage.StageInfo;

/**
 * Provides contextual information of the running batch.
 * <p>
 * Clients can use this class <em>only in operator methods</em>.
 * </p>
 *
 * <h3> requirements </h3>
 * <p>
 * This API requires that {@link StageInfo} object has been registered to {@link ResourceBroker}.
 * </p>
 */
public final class BatchContext {

    private BatchContext() {
        return;
    }

    /**
     * Returns a batch argument.
     * @param name the argument name
     * @return the corresponded value, or {@code null} if the target argument is not defined
     * @throws IllegalArgumentException if the parameter is {@code null}
     * @throws IllegalStateException if the current session is wrong
     */
    public static String get(String name) {
        if (name == null) {
            throw new IllegalArgumentException("name must not be null"); //$NON-NLS-1$
        }
        StageInfo info = ResourceBroker.find(StageInfo.class);
        if (info == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "required resource has not been prepared yet: {0}",
                    StageInfo.class));
        }
        return info.getBatchArguments().get(name);
    }
}
