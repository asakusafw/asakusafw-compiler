package com.asakusafw.bridge.hadoop;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compatibility APIs.
 */
public final class Compatibility {

    static final Logger LOG = LoggerFactory.getLogger(Compatibility.class);

    private static final Method JOB_CONTEXT_GET_CONFIGURATION;
    static {
        Method method;
        try {
            method = JobContext.class.getMethod("getConfiguration"); //$NON-NLS-1$
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
        JOB_CONTEXT_GET_CONFIGURATION = method;
    }

    private Compatibility() {
        return;
    }

    /**
     * Invokes {@link JobContext#getConfiguration()}.
     * @param context the target context
     * @return the invocation result
     */
    public static Configuration getConfiguration(JobContext context) {
        if (JOB_CONTEXT_GET_CONFIGURATION != null) {
            try {
                return (Configuration) JOB_CONTEXT_GET_CONFIGURATION.invoke(context);
            } catch (ReflectiveOperationException e) {
                LOG.warn("failed to invoke compatible operation", e);
            }
        }
        return context.getConfiguration();
    }
}
