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
package com.asakusafw.vanilla.testkit.inprocess;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.asakusafw.bridge.launch.LaunchConfigurationException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.testdriver.TestDriverContext;
import com.asakusafw.testdriver.TestExecutionPlan;
import com.asakusafw.testdriver.hadoop.ConfigurationFactory;
import com.asakusafw.testdriver.inprocess.CommandEmulator;
import com.asakusafw.testdriver.inprocess.EmulatorUtils;
import com.asakusafw.vanilla.client.VanillaLauncher;
import com.asakusafw.vanilla.compiler.common.VanillaTask;

/**
 * A {@link CommandEmulator} for Asakusa Vanilla.
 * @since 0.4.0
 */
public class VanillaCommandEmulator extends CommandEmulator {

    static final Location CORE_CONFIG = Location.of("core/conf/asakusa-resources.xml"); //$NON-NLS-1$

    static final int ARG_BATCH_ID = 0;

    static final int ARG_FLOW_ID = ARG_BATCH_ID + 1;

    static final int ARG_EXECUTION_ID = ARG_FLOW_ID + 1;

    static final int ARG_ARGUMENTS = ARG_EXECUTION_ID + 1;

    static final int ARG_APPLICATION = ARG_ARGUMENTS + 1;

    static final int MINIMUM_TOKENS = ARG_APPLICATION + 1;

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

    @Override
    public String getName() {
        return VanillaTask.MODULE_NAME;
    }

    @Override
    public boolean accepts(
            TestDriverContext context, ConfigurationFactory configurations,
            TestExecutionPlan.Command command) {
        if (command.getModuleName().equals(VanillaTask.MODULE_NAME) == false) {
            return false;
        }
        List<String> cmd = command.getCommandTokens();
        if (cmd.size() < MINIMUM_TOKENS) {
            return false;
        }
        if (EmulatorUtils.hasCommandSuffix(cmd.get(0), VanillaTask.PATH_COMMAND.toPath()) == false) {
            return false;
        }
        return true;
    }

    @Override
    public void execute(
            TestDriverContext context, ConfigurationFactory configurations,
            TestExecutionPlan.Command command) throws IOException, InterruptedException {
        List<String> arguments = getLaunchArguments(context, command);
        try (ClassLoaderContext cl = new ClassLoaderContext(loader(context))) {
            Configuration hadoop = configurations.newInstance();
            hadoop.setClassLoader(cl.getClassLoader());
            int exit = VanillaLauncher.exec(hadoop, arguments.toArray(new String[arguments.size()]));
            if (exit != 0) {
                throw new IOException(MessageFormat.format(
                        "Asakusa Vanilla returned non-zero exit status: {0}",
                        exit));
            }
        } catch (LaunchConfigurationException e) {
            throw new IOException("Asakusa Vanilla configuration was failed", e);
        }
    }

    private static List<String> getLaunchArguments(TestDriverContext context, TestExecutionPlan.Command command) {
        LinkedList<String> rest = new LinkedList<>(command.getCommandTokens());

        rest.removeFirst(); // command
        String batchId = rest.removeFirst();
        String flowId = rest.removeFirst();
        String executionId = rest.removeFirst();
        String batchArguments = rest.removeFirst();
        String application = rest.removeFirst();

        File hadoopConf = new File(context.getFrameworkHomePath(), CORE_CONFIG.toPath());
        File engineConf = new File(context.getFrameworkHomePath(), VanillaTask.PATH_ENGINE_CONFIG.toPath());

        List<String> results = new ArrayList<>();
        Collections.addAll(results, "--client", application); //$NON-NLS-1$
        Collections.addAll(results, "--batch-id", batchId); //$NON-NLS-1$
        Collections.addAll(results, "--flow-id", flowId); //$NON-NLS-1$
        Collections.addAll(results, "--execution-id", executionId); //$NON-NLS-1$
        Collections.addAll(results, "--batch-arguments", batchArguments); //$NON-NLS-1$
        results.addAll(toKeyValueOptions("--hadoop-conf", DEFAULT_HADOOP_CONF)); //$NON-NLS-1$
        if (hadoopConf.isFile()) {
            Collections.addAll(results, "--hadoop-conf", "@" + hadoopConf.getAbsolutePath()); //$NON-NLS-1$
        }
        results.addAll(toKeyValueOptions("--engine-conf", DEFAULT_ENGINE_CONF)); //$NON-NLS-1$
        if (engineConf.isFile()) {
            Collections.addAll(results, "--engine-conf", "@" + engineConf.getAbsolutePath()); //$NON-NLS-1$
        }
        // add rest arguments
        results.addAll(rest);
        return results;
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

    private static URLClassLoader loader(TestDriverContext context) throws MalformedURLException {
        List<URL> libraries = new ArrayList<>();
        libraries.add(new File(context.getFrameworkHomePath(), VanillaTask.PATH_CONFIG_DIR.toPath()).toURI().toURL());
        libraries.add(EmulatorUtils.getJobflowLibraryPath(context).toURI().toURL());
        for (File file : EmulatorUtils.getBatchLibraryPaths(context)) {
            libraries.add(file.toURI().toURL());
        }
        return URLClassLoader.newInstance(
                libraries.toArray(new URL[libraries.size()]),
                context.getClassLoader());
    }
}
