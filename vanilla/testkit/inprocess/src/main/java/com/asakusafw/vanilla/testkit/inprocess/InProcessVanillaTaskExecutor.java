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
package com.asakusafw.vanilla.testkit.inprocess;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.launch.LaunchConfigurationException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.vanilla.client.VanillaLauncher;
import com.asakusafw.vanilla.compiler.common.VanillaTask;
import com.asakusafw.vanilla.testkit.common.VanillaTaskInfo;
import com.asakusafw.vanilla.testkit.common.VanillaTaskInfo.Requiremnt;
import com.asakusafw.workflow.executor.TaskExecutionContext;
import com.asakusafw.workflow.executor.TaskExecutor;
import com.asakusafw.workflow.executor.TaskExecutors;
import com.asakusafw.workflow.model.CommandTaskInfo;
import com.asakusafw.workflow.model.TaskInfo;

/**
 * Executes Asakusa Vanilla tasks on the testing process.
 * This only supports tasks which have {@link VanillaTaskInfo} as their attribute.
 * @since 0.10.0
 */
public class InProcessVanillaTaskExecutor implements TaskExecutor {

    static final Logger LOG = LoggerFactory.getLogger(InProcessVanillaTaskExecutor.class);

    private static final Map<String, String> DEFAULT_ENGINE_CONF;
    static {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("com.asakusafw.dag.view.validate", "TYPE"); //$NON-NLS-1$ //$NON-NLS-2$
        DEFAULT_ENGINE_CONF = Collections.unmodifiableMap(map);
    }

    private static final Map<String, String> DEFAULT_HADOOP_CONF;
    static {
        Map<String, String> map = new LinkedHashMap<>();
        DEFAULT_HADOOP_CONF = Collections.unmodifiableMap(map);
    }

    private final Map<String, String> engineConfigurations = new LinkedHashMap<>();

    /**
     * Adds an engine configuration.
     * @param key the configuration key
     * @param value the configuration value
     * @return this
     */
    public InProcessVanillaTaskExecutor withEngine(String key, String value) {
        engineConfigurations.put(key, value);
        return this;
    }

    @Override
    public boolean isSupported(TaskExecutionContext context, TaskInfo task) {
        VanillaTaskInfo info = getVanillaTaskInfo(task);
        if (info == null) {
            return false;
        }
        assert task instanceof CommandTaskInfo;
        assert Objects.equals(task.getModuleName(), VanillaTask.MODULE_NAME);

        Set<Requiremnt> requirements = info.getRequirements();
        LOG.debug("vanilla requirements: {}", requirements);

        if (requirements.contains(Requiremnt.CORE_CONFIGURATION_FILE)) {
            if (TaskExecutors.findCoreConfigurationFile(context)
                    .filter(Files::isRegularFile)
                    .isPresent() == false) {
                LOG.debug("vanilla skipped - core configuration file does not exist");
                return false;
            }
        }
        if (requirements.contains(Requiremnt.ENGINE_CONFIGURATION_FILE)) {
            if (TaskExecutors.findFrameworkFile(context, VanillaTask.PATH_ENGINE_CONFIG.toPath())
                    .filter(Files::isRegularFile)
                    .isPresent() == false) {
                LOG.debug("vanilla skipped - engine configuration file does not exist");
                return false;
            }
        }
        return true;
    }

    @Override
    public void execute(TaskExecutionContext context, TaskInfo task) throws InterruptedException, IOException {
        assert task instanceof CommandTaskInfo;
        List<String> arguments = getLaunchArguments(context, (CommandTaskInfo) task);
        TaskExecutors.withLibraries(context, classLoader -> {
            Configuration hadoop = context.findResource(Configuration.class).get();
            hadoop.setClassLoader(classLoader);
            try {
                int code = VanillaLauncher.exec(hadoop, arguments.stream().toArray(String[]::new));
                if (code != 0) {
                    throw new IOException(MessageFormat.format(
                            "unexpected exit status: task={0}, status={1}",
                            task,
                            code));
                }
            } catch (LaunchConfigurationException e) {
                throw new IOException(MessageFormat.format(
                        "error occurred while launching Asakusa Vanilla: args={0}",
                        arguments), e);
            }
        });
    }

    private List<String> getLaunchArguments(TaskExecutionContext context, CommandTaskInfo command) {
        LinkedList<String> rest = new LinkedList<>(
                TaskExecutors.resolveCommandTokens(context, command.getArguments(context.getConfigurations())));

        String batchId = rest.removeFirst();
        String flowId = rest.removeFirst();
        String executionId = rest.removeFirst();
        String batchArguments = rest.removeFirst();
        String application = rest.removeFirst();

        Optional<Path> hadoopConf = TaskExecutors.findCoreConfigurationFile(context)
                .filter(Files::isRegularFile);
        Optional<Path> engineConf = TaskExecutors.findFrameworkFile(context, VanillaTask.PATH_ENGINE_CONFIG.toPath())
                .filter(Files::isRegularFile);

        List<String> results = new ArrayList<>();
        Collections.addAll(results, "--client", application); //$NON-NLS-1$
        Collections.addAll(results, "--batch-id", batchId); //$NON-NLS-1$
        Collections.addAll(results, "--flow-id", flowId); //$NON-NLS-1$
        Collections.addAll(results, "--execution-id", executionId); //$NON-NLS-1$
        Collections.addAll(results, "--batch-arguments", batchArguments); //$NON-NLS-1$
        results.addAll(toKeyValueOptions("--hadoop-conf", DEFAULT_HADOOP_CONF)); //$NON-NLS-1$
        hadoopConf.ifPresent(it -> Collections.addAll(results,
                "--hadoop-conf", "@" + it.toAbsolutePath().toString()));
        results.addAll(toKeyValueOptions("--engine-conf", DEFAULT_ENGINE_CONF)); //$NON-NLS-1$
        engineConf.ifPresent(it -> Collections.addAll(results,
                "--engine-conf", "@" + it.toAbsolutePath().toString()));
        results.addAll(toKeyValueOptions("--engine-conf", engineConfigurations)); //$NON-NLS-1$
        // add rest arguments
        results.addAll(rest);
        return results;
    }

    private static VanillaTaskInfo getVanillaTaskInfo(TaskInfo task) {
        if ((task instanceof CommandTaskInfo) == false) {
            return null;
        }
        return task.findAttribute(VanillaTaskInfo.class).orElseGet(() -> Optional.of(task)
                .map(it -> (CommandTaskInfo) it)
                .filter(it -> Objects.equals(it.getModuleName(), VanillaTask.MODULE_NAME))
                .filter(it -> Objects.equals(it.getProfileName(), VanillaTask.PROFILE_NAME))
                .filter(it -> Objects.equals(Location.of(it.getCommand()), VanillaTask.PATH_COMMAND))
                .map(it -> new VanillaTaskInfo(EnumSet.allOf(VanillaTaskInfo.Requiremnt.class)))
                .orElse(null));
    }

    private static List<String> toKeyValueOptions(String prefix, Map<String, String> pairs) {
        List<String> results = new ArrayList<>();
        pairs.forEach((k, v) -> {
            results.add(prefix);
            if (v == null) {
                results.add(k);
            } else {
                results.add(String.format("%s=%s", k, v)); //$NON-NLS-1$
            }
        });
        return results;
    }

    @Override
    public String toString() {
        return "Vanilla"; //$NON-NLS-1$
    }
}
