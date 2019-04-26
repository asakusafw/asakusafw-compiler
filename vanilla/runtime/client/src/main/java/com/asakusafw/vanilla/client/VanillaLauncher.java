/**
 * Copyright 2011-2019 Asakusa Framework Team.
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

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.bridge.broker.ResourceSession;
import com.asakusafw.bridge.launch.LaunchConfiguration;
import com.asakusafw.bridge.launch.LaunchConfigurationException;
import com.asakusafw.bridge.launch.LaunchInfo;
import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicProcessorContext;
import com.asakusafw.dag.api.processor.extension.ProcessorContextExtension;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
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
 * @version 0.5.3
 */
public class VanillaLauncher {

    static final Logger LOG = LoggerFactory.getLogger(VanillaLauncher.class);

    private final LaunchInfo configuration;

    private final ClassLoader applicationLoader;

    private final Configuration hadoop;

    /**
     * Creates a new instance.
     * @param configuration the launching configuration
     */
    public VanillaLauncher(LaunchInfo configuration) {
        this(Arguments.requireNonNull(configuration), configuration.getStageClient().getClassLoader());
    }

    /**
     * Creates a new instance.
     * @param configuration the launching configuration
     * @param classLoader the application class loader
     */
    public VanillaLauncher(LaunchInfo configuration, ClassLoader classLoader) {
        Arguments.requireNonNull(configuration);
        Arguments.requireNonNull(classLoader);
        this.configuration = configuration;
        this.applicationLoader = classLoader;
        this.hadoop = new Configuration();
        this.hadoop.setClassLoader(classLoader);
    }

    VanillaLauncher(LaunchInfo configuration, Configuration hadoop) {
        Arguments.requireNonNull(configuration);
        Arguments.requireNonNull(hadoop);
        this.configuration = configuration;
        this.hadoop = hadoop;
        this.applicationLoader = hadoop.getClassLoader();
    }

    /**
     * Executes DAG.
     * @return the exit status
     * @see LaunchUtil#EXEC_SUCCESS
     * @see LaunchUtil#EXEC_ERROR
     * @see LaunchUtil#EXEC_INTERRUPTED
     */
    public int exec() {
        BasicProcessorContext context =
                LaunchUtil.createProcessorContext(applicationLoader, configuration, hadoop);
        VanillaConfiguration conf = VanillaConfiguration.extract(context::getProperty);
        GraphInfo graph = LaunchUtil.extract(configuration.getStageClient());
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
            return LaunchUtil.EXEC_SUCCESS;
        } catch (IOException e) {
            LOG.error(MessageFormat.format(
                    "DAG failed: {0}",
                    configuration.getStageInfo()), e);
            return LaunchUtil.EXEC_ERROR;
        } catch (InterruptedException e) {
            LOG.warn(MessageFormat.format(
                    "DAG interrupted: {0}",
                    configuration.getStageInfo()), e);
            return LaunchUtil.EXEC_INTERRUPTED;
        }
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
        showEnvironment();
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
     * Executes the given DAG.
     * @param context the current context
     * @param graph the target DAG
     * @throws IOException if I/O error was occurred while executing the given DAG
     * @throws InterruptedException if interrupted while executing the given DAG
     * @since 0.4.2
     */
    public static void execute(
            ProcessorContext context,
            GraphInfo graph) throws IOException, InterruptedException {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(graph);
        VanillaConfiguration configuration = VanillaConfiguration.extract(context::getProperty);
        execute(context, configuration, graph);
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
        BasicBufferStore.Builder storeBuilder = BasicBufferStore.builder()
                .withDirectory(configuration.getSwapDirectory())
                .withDivision(configuration.getSwapDivision())
                .withDecorator(configuration.getSwapDecorator(context.getClassLoader()));

        GraphMirror mirror = GraphMirror.of(graph);
        VertexScheduler scheduler = new BasicVertexScheduler();
        try (BasicBufferStore store = storeBuilder.build();
                BasicEdgeDriver edges = new BasicEdgeDriver(
                        context.getClassLoader(),
                        mirror,
                        new BasicBufferPool(configuration.getBufferPoolSize(), store),
                        store.getBlobStore(),
                        configuration.getNumberOfPartitions(),
                        configuration.getOutputBufferSize(),
                        configuration.getOutputBufferMargin(),
                        configuration.getNumberOfOutputRecords(),
                        configuration.getMergeThreshold(),
                        configuration.getMergeFactor());
                ResourceSession session = LaunchUtil.attachSession(context, ResourceBroker.Scope.VM)) {
            if (RuntimeContext.get().isSimulation() == false) {
                new GraphExecutor(context, mirror,
                        scheduler, edges,
                        configuration.getNumberOfThreads()).run();
            }
        }
    }

    static void showEnvironment() {
        if (LOG.isDebugEnabled()) {
            showEnvironment(VanillaConstants.ENV_VANILLA_LAUNCHER);
            showEnvironment(VanillaConstants.ENV_VANILLA_OPTIONS);
            showEnvironment(VanillaConstants.ENV_VANILLA_ARGUMENTS);
        }
    }

    private static void showEnvironment(String name) {
        LOG.debug("{}: {}", name, Optional.ofNullable(System.getenv(name)).orElse(""));
    }
}
