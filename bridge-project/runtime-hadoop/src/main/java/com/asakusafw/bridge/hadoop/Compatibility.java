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
     * @deprecated Directly use {@code context.getConfiguration()} instead
     */
    @Deprecated
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
