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
package com.asakusafw.vanilla.compiler.tester;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.asakusafw.bridge.launch.LaunchConfigurationException;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.tester.TesterContext;
import com.asakusafw.lang.compiler.tester.executor.TaskExecutor;
import com.asakusafw.lang.compiler.tester.executor.TaskExecutors;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.vanilla.client.VanillaLauncher;
import com.asakusafw.vanilla.compiler.common.VanillaTask;

/**
 * Executes Asakusa Vanilla tasks on the testing process.
 * @since 0.4.0
 */
public class InProcessVanillaTaskExecutor implements TaskExecutor {

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
    public boolean isSupported(Context context, TaskReference task) {
        return task instanceof CommandTaskReference
                && task.getModuleName().equals(VanillaTask.MODULE_NAME);
    }

    @Override
    public void execute(Context context, TaskReference task) throws InterruptedException, IOException {
        assert task instanceof CommandTaskReference;
        List<String> arguments = getLaunchArguments(context, (CommandTaskReference) task);

        File jobflow = TaskExecutors.getJobflowLibrary(context);
        try (URLClassLoader loader = getLaunchClassLoader(context, jobflow)) {
            try {
                int code = VanillaLauncher.exec(loader, arguments.stream().toArray(String[]::new));
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
        }
    }

    private List<String> getLaunchArguments(Context context, CommandTaskReference command) {
        LinkedList<String> rest = new LinkedList<>(
                TaskExecutors.resolveCommandTokens(context, command.getArguments()));

        String batchId = rest.removeFirst();
        String flowId = rest.removeFirst();
        String executionId = rest.removeFirst();
        String batchArguments = rest.removeFirst();
        String application = rest.removeFirst();

        File hadoopConf = TaskExecutors.getCoreConfigurationFile(context);
        File engineConf = TaskExecutors.getFrameworkFile(context, VanillaTask.PATH_ENGINE_CONFIG);

        List<String> results = new ArrayList<>();
        Collections.addAll(results, "--client", application); //$NON-NLS-1$
        Collections.addAll(results, "--batch-id", batchId); //$NON-NLS-1$
        Collections.addAll(results, "--flow-id", flowId); //$NON-NLS-1$
        Collections.addAll(results, "--execution-id", executionId); //$NON-NLS-1$
        Collections.addAll(results, "--batch-arguments", batchArguments); //$NON-NLS-1$
        if (hadoopConf.isFile()) {
            Collections.addAll(results, "--hadoop-conf", "@" + hadoopConf.getAbsolutePath()); //$NON-NLS-1$
        }
        if (engineConf.isFile()) {
            Collections.addAll(results, "--engine-conf", "@" + engineConf.getAbsolutePath()); //$NON-NLS-1$
        }
        // add rest arguments
        results.addAll(rest);

        engineConfigurations.forEach((k, v) -> {
            Collections.addAll(results, "--engine-conf", String.format("%s=%s", k, v)); //$NON-NLS-1$ //$NON-NLS-2$
        });
        return results;
    }

    private static URLClassLoader getLaunchClassLoader(Context context, File... libraries) {
        return loader(context.getTesterContext(), Arrays.asList(libraries));
    }

    private static URLClassLoader loader(TesterContext context, List<File> libraries) {
        return Lang.safe(() -> URLClassLoader.newInstance(
                libraries.stream()
                    .map(File::toURI)
                    .map(uri -> Invariants.safe(() -> uri.toURL()))
                    .toArray(URL[]::new),
                context.getClassLoader()));
    }

    @Override
    public String toString() {
        return "Vanilla"; //$NON-NLS-1$
    }
}
