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
package com.asakusafw.bridge.launch;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.stage.StageInfo;

/**
 * Provides launching configuration.
 */
public class LaunchConfiguration {

    static final Logger LOG = LoggerFactory.getLogger(LaunchConfiguration.class);

    private final Class<?> stageClient;

    private final StageInfo stageInfo;

    private final Map<String, String> hadoopProperties;

    private final Map<String, String> engineProperties;

    /**
     * Creates a new instance.
     * @param stageClient the stage client class
     * @param stageInfo the stage information
     * @param hadoopProperties Hadoop platform properties
     * @param engineProperties stage engine properties
     */
    public LaunchConfiguration(
            Class<?> stageClient,
            StageInfo stageInfo,
            Map<String, String> hadoopProperties,
            Map<String, String> engineProperties) {
        this.stageClient = stageClient;
        this.stageInfo = stageInfo;
        this.hadoopProperties = Collections.unmodifiableMap(new LinkedHashMap<>(hadoopProperties));
        this.engineProperties = Collections.unmodifiableMap(new LinkedHashMap<>(engineProperties));
    }

    /**
     * Analyzes launch arguments and returns {@link LaunchConfiguration} from them.
     * @param classLoader the current class loader
     * @param arguments the launch arguments
     * @return the analyzed configuration
     * @throws LaunchConfigurationException if arguments are wrong
     * @see #parse(ClassLoader, List, LaunchOption...)
     */
    public static LaunchConfiguration parse(
            ClassLoader classLoader,
            String... arguments) throws LaunchConfigurationException {
        return parse(classLoader, Arrays.asList(arguments));
    }

    /**
     * Analyzes launch arguments and returns {@link LaunchConfiguration} from them.
     * @param classLoader the current class loader
     * @param arguments the launch arguments
     * @param extraOptions the extra launch options
     * @return the analyzed configuration
     * @throws LaunchConfigurationException if arguments are wrong
     */
    public static LaunchConfiguration parse(
            ClassLoader classLoader,
            List<String> arguments,
            LaunchOption<?>... extraOptions) throws LaunchConfigurationException {
        LaunchOption<Class<?>> stageClient = new StageClientOption(classLoader);
        LaunchOption<StageInfo> stageInfo = new StageInfoOption();
        LaunchOption<Map<String, String>> hadoopProperties = new HadoopPropertiesOption();
        LaunchOption<Map<String, String>> engineProperties = new EnginePropertiesOption();

        List<LaunchOption<?>> options = new ArrayList<>();
        Collections.addAll(options, stageClient, stageInfo, hadoopProperties, engineProperties);
        Collections.addAll(options, extraOptions);

        acceptAll(options, arguments);
        return new LaunchConfiguration(
                stageClient.resolve(),
                stageInfo.resolve(),
                hadoopProperties.resolve(),
                engineProperties.resolve());
    }

    /**
     * Accepts all arguments.
     * @param options the {@link LaunchOption}s which accept arguments
     * @param arguments the target arguments
     * @throws LaunchConfigurationException if arguments are not valid
     */
    public static void acceptAll(
            Collection<? extends LaunchOption<?>> options,
            List<String> arguments) throws LaunchConfigurationException {
        Map<String, LaunchOption<?>> commands = toCommandMap(options);
        LinkedList<String> rest = new LinkedList<>(arguments);
        while (rest.isEmpty() == false) {
            String command = rest.removeFirst();
            if (command.isEmpty()) {
                continue;
            }
            LaunchOption<?> option = commands.get(command);
            if (option == null) {
                throw new LaunchConfigurationException(MessageFormat.format(
                        "unknown command: {0}",
                        command));
            }
            if (rest.isEmpty()) {
                throw new LaunchConfigurationException(MessageFormat.format(
                        "missing command value: {0}",
                        command));
            }
            String value = rest.removeFirst();
            LOG.debug("launch option: {} {}", command, value); //$NON-NLS-1$

            option.accept(command, value);
        }
    }

    private static Map<String, LaunchOption<?>> toCommandMap(Iterable<? extends LaunchOption<?>> options) {
        Map<String, LaunchOption<?>> commands = new HashMap<>();
        for (LaunchOption<?> option : options) {
            for (String command : option.getCommands()) {
                if (commands.containsKey(command)) {
                    throw new IllegalStateException(MessageFormat.format(
                            "conflict command binding: {0}",
                            command));
                }
                commands.put(command, option);
            }
        }
        return commands;
    }

    /**
     * Returns the stage client class.
     * @return the stage client class
     */
    public Class<?> getStageClient() {
        return stageClient;
    }

    /**
     * Returns the stage information.
     * @return the stage information
     */
    public StageInfo getStageInfo() {
        return stageInfo;
    }

    /**
     * Returns the properties for Hadoop platform.
     * @return the Hadoop properties
     */
    public Map<String, String> getHadoopProperties() {
        return hadoopProperties;
    }

    /**
     * Returns the properties for stage engine.
     * @return the properties for stage engine
     */
    public Map<String, String> getEngineProperties() {
        return engineProperties;
    }
}
