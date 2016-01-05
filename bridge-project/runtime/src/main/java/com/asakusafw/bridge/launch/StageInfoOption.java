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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.asakusafw.bridge.stage.StageInfo;

/**
 * A {@link LaunchOption} about {@link StageInfo}.
 */
public class StageInfoOption implements LaunchOption<StageInfo> {

    /**
     * The command name for user name.
     */
    public static final String COMMAND_USER_NAME = "--user-name"; //$NON-NLS-1$

    /**
     * The command name for batch ID.
     */
    public static final String COMMAND_BATCH_ID = "--batch-id"; //$NON-NLS-1$

    /**
     * The command name for flow ID.
     */
    public static final String COMMAND_FLOW_ID = "--flow-id"; //$NON-NLS-1$

    /**
     * The command name for stage ID.
     */
    public static final String COMMAND_STAGE_ID = "--stage-id"; //$NON-NLS-1$

    /**
     * The command name for execution ID.
     */
    public static final String COMMAND_EXECUTION_ID = "--execution-id"; //$NON-NLS-1$

    /**
     * The command name for batch arguments.
     */
    public static final String COMMAND_BATCH_ARGUMENTS = "--batch-arguments"; //$NON-NLS-1$

    private static final String[] MANDATORY = { COMMAND_BATCH_ID, COMMAND_FLOW_ID, COMMAND_EXECUTION_ID };

    private final String defaultUserName;

    private final Map<String, String> entries = new LinkedHashMap<>();

    /**
     * Creates a new instance.
     */
    public StageInfoOption() {
        this(System.getProperty("user.name")); //$NON-NLS-1$
    }

    /**
     * Creates a new instance.
     * @param defaultUserName the default user name (nullable)
     */
    public StageInfoOption(String defaultUserName) {
        this.defaultUserName = defaultUserName;
    }



    @Override
    public Set<String> getCommands() {
        Set<String> results = new LinkedHashSet<>();
        results.add(COMMAND_USER_NAME);
        results.add(COMMAND_BATCH_ID);
        results.add(COMMAND_FLOW_ID);
        results.add(COMMAND_STAGE_ID);
        results.add(COMMAND_EXECUTION_ID);
        results.add(COMMAND_BATCH_ARGUMENTS);
        return Collections.unmodifiableSet(results);
    }

    @Override
    public void accept(String command, String value) throws LaunchConfigurationException {
        if (entries.containsKey(command)) {
            throw new LaunchConfigurationException(MessageFormat.format(
                    "duplicate stage info: {0} {1}",
                    command, value));
        }
        entries.put(command, value);
    }

    @Override
    public StageInfo resolve() throws LaunchConfigurationException {
        for (String command : MANDATORY) {
            if (entries.containsKey(command) == false) {
                throw new LaunchConfigurationException(MessageFormat.format(
                        "missing stage info: {0} {1}",
                        command));
            }
        }
        String userName = Objects.toString(entries.get(COMMAND_USER_NAME), defaultUserName);
        String batchId = entries.get(COMMAND_BATCH_ID);
        String flowId = entries.get(COMMAND_FLOW_ID);
        String stageId = entries.get(COMMAND_STAGE_ID);
        String executionId = entries.get(COMMAND_EXECUTION_ID);
        String batchArguments = entries.get(COMMAND_BATCH_ARGUMENTS);

        return new StageInfo(userName, batchId, flowId, stageId, executionId, batchArguments);
    }
}
