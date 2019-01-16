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
package com.asakusafw.bridge.launch;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.bridge.stage.StageInfo;

/**
 * A {@link LaunchOption} which handles (extra) batch arguments.
 * @since 0.4.1
 */
public class BatchArgumentsOption implements LaunchOption<Map<String, String>> {

    private final Set<String> commandNames;

    private final Map<String, String> arguments = new LinkedHashMap<>();

    /**
     * Creates a new instance.
     * @param commandNames the command names
     */
    public BatchArgumentsOption(String... commandNames) {
        this(Arrays.asList(commandNames));
    }

    /**
     * Creates a new instance.
     * @param commandNames the command names
     */
    public BatchArgumentsOption(Collection<String> commandNames) {
        this.commandNames = Collections.unmodifiableSet(new LinkedHashSet<>(commandNames));
    }

    @Override
    public Set<String> getCommands() {
        return commandNames;
    }

    @Override
    public void accept(String command, String value) throws LaunchConfigurationException {
        int delimiter = value.indexOf('=');
        String argKey;
        String argValue;
        if (delimiter < 0) {
            argKey = value;
            argValue = "true";
        } else {
            argKey = value.substring(0, delimiter);
            argValue = value.substring(delimiter + 1);
        }
        if (arguments.putIfAbsent(argKey, argValue) != null) {
            throw new LaunchConfigurationException(MessageFormat.format(
                    "duplicate batch argument: {0} {1}",
                    command,
                    argKey));
        }
        arguments.put(argKey, argValue);
    }

    @Override
    public Map<String, String> resolve() throws LaunchConfigurationException {
        return Collections.unmodifiableMap(new LinkedHashMap<>(arguments));
    }

    /**
     * Merges the arguments into {@link StageInfo}.
     * @param info the target {@link StageInfo}
     * @return the merged {@link StageInfo}
     */
    public StageInfo mergeTo(StageInfo info) {
        if (arguments.isEmpty()) {
            return info;
        }
        Map<String, String> merged = new LinkedHashMap<>(info.getBatchArguments());
        merged.putAll(arguments);
        return new StageInfo(
                info.getUserName(),
                info.getBatchId(), info.getFlowId(), info.getStageId(), info.getExecutionId(),
                merged);
    }
}
