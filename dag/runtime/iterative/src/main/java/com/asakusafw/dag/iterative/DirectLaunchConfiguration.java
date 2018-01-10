/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.iterative;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.asakusafw.bridge.launch.BatchArgumentsOption;
import com.asakusafw.bridge.launch.LaunchConfiguration;
import com.asakusafw.bridge.launch.LaunchConfigurationException;
import com.asakusafw.bridge.launch.LaunchInfo;
import com.asakusafw.bridge.launch.LaunchOption;
import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.iterative.common.BaseCursor;
import com.asakusafw.iterative.common.IterativeExtensions;
import com.asakusafw.iterative.common.ParameterTable;
import com.asakusafw.iterative.launch.IterativeStageInfo;

/**
 * Provides launching configuration for iterative batches.
 * @since 0.4.1
 */
public final class DirectLaunchConfiguration {

    private final LaunchConfiguration origin;

    private final ParameterTable parameterTable;

    private DirectLaunchConfiguration(LaunchConfiguration origin, ParameterTable parameterTable) {
        this.origin = origin;
        this.parameterTable = parameterTable != null ? parameterTable : IterativeExtensions.builder().build();
    }

    /**
     * Analyzes launch arguments and returns {@link DirectLaunchConfiguration} from them.
     * @param classLoader the current class loader
     * @param arguments the launch arguments
     * @return the analyzed configuration
     * @throws LaunchConfigurationException if arguments are wrong
     */
    public static DirectLaunchConfiguration parse(
            ClassLoader classLoader,
            String... arguments) throws LaunchConfigurationException {
        return parse(classLoader, Arrays.asList(arguments));
    }

    /**
     * Analyzes launch arguments and returns {@link DirectLaunchConfiguration} from them.
     * @param classLoader the current class loader
     * @param arguments the launch arguments
     * @param extraOptions the extra launch options
     * @return the analyzed configuration
     * @throws LaunchConfigurationException if arguments are wrong
     */
    public static DirectLaunchConfiguration parse(
            ClassLoader classLoader,
            List<String> arguments,
            LaunchOption<?>... extraOptions) throws LaunchConfigurationException {
        BatchArgumentsOption batchArguments = new BatchArgumentsOption("-A", "--argument");
        LaunchOption<ParameterTable> parameterTable = new DirectParameterTableOption("-AA", "--parameter-table");
        List<LaunchOption<?>> options = new ArrayList<>();
        Collections.addAll(options, batchArguments);
        Collections.addAll(options, parameterTable);
        Collections.addAll(options, extraOptions);
        LaunchOption<?>[] optionArray = options.toArray(new LaunchOption<?>[options.size()]);
        LaunchConfiguration origin = LaunchConfiguration.parse(classLoader, arguments, optionArray);
        return new DirectLaunchConfiguration(
                new LaunchConfiguration(
                        origin.getStageClient(),
                        batchArguments.mergeTo(origin.getStageInfo()),
                        origin.getHadoopProperties(),
                        origin.getEngineProperties()),
                parameterTable.resolve());
    }

    /**
     * Returns a new cursor of {@link LaunchInfo}.
     * @return the created cursor
     */
    public Cursor newCursor() {
        return new Cursor(origin, getStageInfo());
    }


    /**
     * Returns the stage information.
     * @return the stage information
     */
    public IterativeStageInfo getStageInfo() {
        EnumSet<IterativeStageInfo.Option> options = EnumSet.noneOf(IterativeStageInfo.Option.class);
        if (parameterTable.getRowCount() >= 2) {
            options.add(IterativeStageInfo.Option.QUALIFY_EXECUTION_ID);
        }
        return new IterativeStageInfo(origin.getStageInfo(), parameterTable, options);
    }

    /**
     * A {@link LaunchInfo} cursor of {@link DirectLaunchConfiguration}.
     * @since 0.4.1
     */
    public static class Cursor implements BaseCursor<LaunchInfo> {

        private final LaunchConfiguration origin;

        private final IterativeStageInfo.Cursor cursor;

        Cursor(LaunchConfiguration origin, com.asakusafw.iterative.launch.IterativeStageInfo stages) {
            this.origin = origin;
            this.cursor = stages.newCursor();
        }

        @Override
        public boolean next() {
            return cursor.next();
        }

        @Override
        public LaunchInfo get() {
            return new Info(origin, cursor.get());
        }
    }

    private static final class Info implements LaunchInfo {

        private final LaunchConfiguration origin;

        private final StageInfo stage;

        Info(LaunchConfiguration origin, StageInfo stage) {
            this.origin = origin;
            this.stage = stage;
        }

        @Override
        public StageInfo getStageInfo() {
            return stage;
        }

        @Override
        public Class<?> getStageClient() {
            return origin.getStageClient();
        }

        @Override
        public Map<String, String> getHadoopProperties() {
            return origin.getHadoopProperties();
        }

        @Override
        public Map<String, String> getEngineProperties() {
            return origin.getEngineProperties();
        }
    }
}
