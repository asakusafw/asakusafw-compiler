/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.vanilla.client;

import static com.asakusafw.vanilla.client.VanillaConstants.*;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.api.activate.ApiActivator;
import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.broker.ResourceSession;
import com.asakusafw.bridge.launch.LaunchInfo;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicProcessorContext;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.runtime.core.HadoopConfiguration;
import com.asakusafw.runtime.core.ResourceConfiguration;

/**
 * Utilities about launching DAG applications.
 * @since 0.4.2
 */
public final class LaunchUtil {

    static final Logger LOG = LoggerFactory.getLogger(LaunchUtil.class);

    private static final Pattern SENSITIVE_KEY = Pattern.compile("pass", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$

    private static final String SENSITIVE_VALUE_MASK = "****"; //$NON-NLS-1$

    /**
     * Exit status: execution was successfully completed.
     */
    public static final int EXEC_SUCCESS = 0;

    /**
     * Exit status: execution was finished with error.
     */
    public static final int EXEC_ERROR = 1;

    /**
     * Exit status: execution was interrupted.
     */
    public static final int EXEC_INTERRUPTED = 2;

    private LaunchUtil() {
        return;
    }

    /**
     * Extracts {@link GraphInfo} from the specified class.
     * @param entry the target class
     * @return the loaded graph
     * @throws IllegalStateException if failed to extract DAG from the target class
     */
    public static GraphInfo extract(Class<?> entry) {
        Arguments.requireNonNull(entry);
        try {
            Object object = entry.newInstance();
            Invariants.require(object instanceof Supplier<?>, () -> MessageFormat.format(
                    "entry class must be a Supplier: {0}",
                    object.getClass().getName()));
            Object info = ((Supplier<?>) object).get();
            Invariants.require(info instanceof GraphInfo, () -> MessageFormat.format(
                    "entry class must supply GraphInfo: {0}",
                    object.getClass().getName(),
                    Optionals.of(info).map(Object::getClass).map(Class::getName).orElse(null)));
            return (GraphInfo) info;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(MessageFormat.format(
                    "exception occurred while loading DAG: {0}",
                    entry), e);
        }
    }

    /**
     * Creates a new {@link ProcessorContext} for executing generic DAG applications.
     * @param classLoader the current applications class loader
     * @param launchInfo the launching information
     * @param hadoop the hadoop configuration
     * @return the created context
     * @since 0.4.2
     */
    public static BasicProcessorContext createProcessorContext(
            ClassLoader classLoader, LaunchInfo launchInfo, Configuration hadoop) {
        BasicProcessorContext context = new BasicProcessorContext(classLoader);
        launchInfo.getEngineProperties().forEach((k, v) -> {
            if (k.startsWith(KEY_HADOOP_PREFIX) == false) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Engine configuration: {}={}", k, shadow(k, v));
                }
                context.withProperty(k, v);
            }
        });

        launchInfo.getHadoopProperties().forEach((k, v) -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Hadoop configuration: {}={}", k, shadow(k, v));
            }
            hadoop.set(k, v);
        });
        launchInfo.getEngineProperties().forEach((k, v) -> {
            if (k.startsWith(KEY_HADOOP_PREFIX)) {
                String subKey = k.substring(KEY_HADOOP_PREFIX.length());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Hadoop configuration: {}={}", subKey, shadow(subKey, v));
                }
                hadoop.set(subKey, v);
            }
        });

        context.withResource(StageInfo.class, launchInfo.getStageInfo());
        context.withResource(Configuration.class, hadoop);
        return context;
    }

    private static String shadow(String key, String value) {
        if (SENSITIVE_KEY.matcher(key).find()) {
            return SENSITIVE_VALUE_MASK;
        }
        return value;
    }

    /**
     * Creates or attaches to {@link ResourceSession}.
     * @param context the current context created by
     *      {@link #createProcessorContext(ClassLoader, LaunchInfo, Configuration)}
     * @param scope the target session scope
     * @return the attached session
     * @throws IOException if I/O error occurred while attaching the session
     * @since 0.4.2
     */
    public static ResourceSession attachSession(
            ProcessorContext context, ResourceBroker.Scope scope) throws IOException {
        return ResourceBroker.attach(scope, s -> {
            s.put(StageInfo.class, context.getResource(StageInfo.class)
                    .get());
            s.put(ResourceConfiguration.class, context.getResource(Configuration.class)
                    .map(HadoopConfiguration::new)
                    .get());
            ApiActivator.load(context.getClassLoader()).forEach(a -> s.schedule(a.activate()));
        });
    }
}
