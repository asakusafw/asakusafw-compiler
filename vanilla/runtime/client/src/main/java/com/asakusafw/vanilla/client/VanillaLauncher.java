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
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.api.activate.ApiActivator;
import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.broker.ResourceSession;
import com.asakusafw.bridge.launch.LaunchConfiguration;
import com.asakusafw.bridge.launch.LaunchConfigurationException;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicProcessorContext;
import com.asakusafw.dag.api.processor.extension.ProcessorContextExtension;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.runtime.core.HadoopConfiguration;
import com.asakusafw.runtime.core.ResourceConfiguration;
import com.asakusafw.runtime.core.context.RuntimeContext;
import com.asakusafw.vanilla.core.engine.BasicEdgeDriver;
import com.asakusafw.vanilla.core.engine.BasicVertexScheduler;
import com.asakusafw.vanilla.core.engine.GraphExecutor;
import com.asakusafw.vanilla.core.engine.VertexScheduler;
import com.asakusafw.vanilla.core.io.BasicBufferPool;
import com.asakusafw.vanilla.core.io.BasicBufferStore;
import com.asakusafw.vanilla.core.mirror.GraphMirror;

/**
 * Asakusa Vanilla application entry.
 * @since 0.4.0
 */
public class VanillaLauncher {

    static final Logger LOG = LoggerFactory.getLogger(VanillaLauncher.class);

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

    private final LaunchConfiguration configuration;

    private final ClassLoader applicationLoader;

    private final Configuration hadoop;

    /**
     * Creates a new instance.
     * @param configuration the launching configuration
     */
    public VanillaLauncher(LaunchConfiguration configuration) {
        this(Arguments.requireNonNull(configuration), configuration.getStageClient().getClassLoader());
    }

    /**
     * Creates a new instance.
     * @param configuration the launching configuration
     * @param classLoader the application class loader
     */
    public VanillaLauncher(LaunchConfiguration configuration, ClassLoader classLoader) {
        Arguments.requireNonNull(configuration);
        Arguments.requireNonNull(classLoader);
        this.configuration = configuration;
        this.applicationLoader = classLoader;
        this.hadoop = new Configuration();
    }

    private VanillaLauncher(LaunchConfiguration configuration, Configuration hadoop) {
        Arguments.requireNonNull(configuration);
        Arguments.requireNonNull(hadoop);
        this.configuration = configuration;
        this.hadoop = hadoop;
        this.applicationLoader = hadoop.getClassLoader();
    }

    /**
     * Executes DAG.
     * @return the exit status
     * @see #EXEC_SUCCESS
     * @see #EXEC_ERROR
     * @see #EXEC_INTERRUPTED
     */
    public int exec() {
        BasicProcessorContext context = newContext();
        VanillaConfiguration conf = VanillaConfiguration.extract(context::getProperty);
        GraphInfo graph = extract(configuration.getStageClient());
        try (InterruptibleIo extension = extend(context)) {
            long start = System.currentTimeMillis();
            LOG.info(MessageFormat.format(
                    "DAG starting: {0}, vertices={1}",
                    configuration.getStageInfo(),
                    graph.getVertices().size()));
            execute(context, conf, graph);
            long finish = System.currentTimeMillis();
            LOG.info(MessageFormat.format(
                    "DAG finished: {0}, vertices={1}, elapsed={2}ms",
                    configuration.getStageInfo(),
                    graph.getVertices().size(),
                    finish - start));
            return EXEC_SUCCESS;
        } catch (IOException e) {
            LOG.error(MessageFormat.format(
                    "DAG failed: {0}",
                    configuration.getStageInfo()), e);
            return EXEC_ERROR;
        } catch (InterruptedException e) {
            LOG.warn(MessageFormat.format(
                    "DAG interrupted: {0}",
                    configuration.getStageInfo()), e);
            return EXEC_INTERRUPTED;
        }
    }

    private BasicProcessorContext newContext() {
        BasicProcessorContext context = new BasicProcessorContext(applicationLoader);
        configuration.getEngineProperties().forEach((k, v) -> {
            if (k.startsWith(KEY_HADOOP_PREFIX) == false) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Engine configuration: {}={}", k, shadow(k, v));
                }
                context.withProperty(k, v);
            }
        });

        configuration.getHadoopProperties().forEach((k, v) -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Hadoop configuration: {}={}", k, shadow(k, v));
            }
            hadoop.set(k, v);
        });
        configuration.getEngineProperties().forEach((k, v) -> {
            if (k.startsWith(KEY_HADOOP_PREFIX)) {
                String subKey = k.substring(KEY_HADOOP_PREFIX.length());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Hadoop configuration: {}={}", subKey, shadow(subKey, v));
                }
                hadoop.set(subKey, v);
            }
        });

        context.withResource(StageInfo.class, configuration.getStageInfo());
        context.withResource(Configuration.class, hadoop);
        return context;
    }

    private static String shadow(String key, String value) {
        if (SENSITIVE_KEY.matcher(key).find()) {
            return SENSITIVE_VALUE_MASK;
        }
        return value;
    }

    private static InterruptibleIo extend(BasicProcessorContext context) throws IOException, InterruptedException {
        ProcessorContextExtension extension = ProcessorContextExtension.load(context.getClassLoader());
        return extension.install(context, context.getEditor());
    }

    /**
     * Program entry.
     * @param args launching configurations
     * @throws LaunchConfigurationException if launching configuration is something wrong
     */
    public static void main(String... args) throws LaunchConfigurationException {
        int status = exec(VanillaLauncher.class.getClassLoader(), args);
        if (status != 0) {
            System.exit(status);
        }
    }

    /**
     * Program entry.
     * @param loader the launch class loader
     * @param args launching configurations
     * @return the exit code
     * @throws LaunchConfigurationException if launching configuration is something wrong
     */
    public static int exec(ClassLoader loader, String... args) throws LaunchConfigurationException {
        RuntimeContext.set(RuntimeContext.DEFAULT.apply(System.getenv()));
        RuntimeContext.get().verifyApplication(loader);

        LaunchConfiguration conf = LaunchConfiguration.parse(loader, Arrays.asList(args));
        VanillaLauncher launcher = new VanillaLauncher(conf, loader);
        return launcher.exec();
    }

    /**
     * Program entry.
     * @param hadoop the context Hadoop configuration
     * @param args launching configurations
     * @return the exit code
     * @throws LaunchConfigurationException if launching configuration is something wrong
     */
    public static int exec(Configuration hadoop, String... args) throws LaunchConfigurationException {
        ClassLoader loader = hadoop.getClassLoader();
        RuntimeContext.set(RuntimeContext.DEFAULT.apply(System.getenv()));
        RuntimeContext.get().verifyApplication(loader);

        LaunchConfiguration conf = LaunchConfiguration.parse(loader, Arrays.asList(args));
        VanillaLauncher launcher = new VanillaLauncher(conf, hadoop);
        return launcher.exec();
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
     * Executes the given DAG.
     * @param context the current context
     * @param configuration the engine configuration
     * @param graph the target DAG
     * @throws IOException if I/O error was occurred while executing the given DAG
     * @throws InterruptedException if interrupted while executing the given DAG
     */
    public static void execute(
            ProcessorContext context,
            VanillaConfiguration configuration,
            GraphInfo graph) throws IOException, InterruptedException {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(configuration);
        Arguments.requireNonNull(graph);
        GraphMirror mirror = GraphMirror.of(graph);
        VertexScheduler scheduler = new BasicVertexScheduler();
        try (BasicBufferStore store = Optionals.of(configuration.getSwapDirectory())
                        .map(BasicBufferStore::new)
                        .orElseGet(BasicBufferStore::new);
                BasicEdgeDriver edges = new BasicEdgeDriver(
                        context.getClassLoader(),
                        mirror,
                        new BasicBufferPool(configuration.getBufferPoolSize(), store),
                        configuration.getNumberOfPartitions(),
                        configuration.getOutputBufferSize(),
                        configuration.getOutputBufferFlush(),
                        configuration.getNumberOfOutputRecords());
                ResourceSession session = ResourceBroker.attach(ResourceBroker.Scope.VM, s -> {
                    s.put(StageInfo.class,
                            context.getResource(StageInfo.class).get());
                    s.put(ResourceConfiguration.class,
                            new HadoopConfiguration(context.getResource(Configuration.class).get()));
                    ApiActivator.load(context.getClassLoader()).forEach(a -> s.schedule(a.activate()));
                })) {
            if (RuntimeContext.get().isSimulation() == false) {
                new GraphExecutor(context, mirror,
                        scheduler, edges,
                        configuration.getNumberOfThreads()).run();
            }
        }
    }
}
