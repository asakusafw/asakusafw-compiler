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
package com.asakusafw.iterative.launch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.asakusafw.bridge.launch.LaunchConfiguration;
import com.asakusafw.bridge.launch.LaunchConfigurationException;
import com.asakusafw.bridge.launch.LaunchOption;
import com.asakusafw.iterative.common.IterativeExtensions;
import com.asakusafw.iterative.common.ParameterTable;

/**
 * Provides launching configuration for iterative batches.
 * @since 0.3.0
 */
public final class IterativeLaunchConfiguration {

    private final LaunchConfiguration origin;

    private final ParameterTable parameterTable;

    private IterativeLaunchConfiguration(LaunchConfiguration origin, ParameterTable parameterTable) {
        this.origin = origin;
        this.parameterTable = parameterTable != null ? parameterTable : IterativeExtensions.builder().build();
    }

    /**
     * Analyzes launch arguments and returns {@link IterativeLaunchConfiguration} from them.
     * @param classLoader the current class loader
     * @param arguments the launch arguments
     * @return the analyzed configuration
     * @throws LaunchConfigurationException if arguments are wrong
     */
    public static IterativeLaunchConfiguration parse(
            ClassLoader classLoader,
            String... arguments) throws LaunchConfigurationException {
        return parse(classLoader, Arrays.asList(arguments));
    }

    /**
     * Analyzes launch arguments and returns {@link IterativeLaunchConfiguration} from them.
     * @param classLoader the current class loader
     * @param arguments the launch arguments
     * @param extraOptions the extra launch options
     * @return the analyzed configuration
     * @throws LaunchConfigurationException if arguments are wrong
     */
    public static IterativeLaunchConfiguration parse(
            ClassLoader classLoader,
            List<String> arguments,
            LaunchOption<?>... extraOptions) throws LaunchConfigurationException {
        LaunchOption<ParameterTable> parameterTable = new ParameterTableOption();
        List<LaunchOption<?>> options = new ArrayList<>();
        Collections.addAll(options, parameterTable);
        Collections.addAll(options, extraOptions);
        LaunchOption<?>[] optionArray = options.toArray(new LaunchOption<?>[options.size()]);
        LaunchConfiguration origin = LaunchConfiguration.parse(classLoader, arguments, optionArray);
        return new IterativeLaunchConfiguration(origin, parameterTable.resolve());
    }

    /**
     * Returns the stage client class.
     * @return the stage client class
     */
    public Class<?> getStageClient() {
        return origin.getStageClient();
    }

    /**
     * Returns the stage information.
     * @return the stage information
     */
    public IterativeStageInfo getStageInfo() {
        return new IterativeStageInfo(origin.getStageInfo(), parameterTable);
    }

    /**
     * Returns the properties for Hadoop platform.
     * @return the Hadoop properties
     */
    public Map<String, String> getHadoopProperties() {
        return origin.getHadoopProperties();
    }

    /**
     * Returns the properties for stage engine.
     * @return the properties for stage engine
     */
    public Map<String, String> getEngineProperties() {
        return origin.getEngineProperties();
    }
}
