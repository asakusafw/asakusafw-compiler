/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.bridge.stage;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.asakusafw.runtime.stage.StageConstants;
import com.asakusafw.runtime.util.VariableTable;

/**
 * Represents stage information.
 */
public class StageInfo {

    /**
     * The key name for storing this class object in serialized form.
     * @see #serialize()
     */
    public static final String KEY_NAME = "com.asakusafw.bridge.stage.info"; //$NON-NLS-1$

    private static final String KEY_BATCH_ARGUMENTS = "args"; //$NON-NLS-1$

    private static final char ESCAPE = '\\';

    private static final char SEPARATOR = '|';

    private final String userName;

    private final String batchId;

    private final String flowId;

    private final String stageId;

    private final String executionId;

    private final Map<String, String> batchArguments;

    private final VariableTable systemVariables;

    private final VariableTable userVariables;

    /**
     * Creates a new instance.
     * @param userName the current user name
     * @param batchId the current batch ID
     * @param flowId the current flow ID
     * @param stageId the current stage ID (nullable)
     * @param executionId the current execution ID
     * @param batchArguments the batch arguments
     */
    public StageInfo(
            String userName,
            String batchId,
            String flowId,
            String stageId,
            String executionId,
            Map<String, String> batchArguments) {
        this.userName = userName;
        this.batchId = batchId;
        this.flowId = flowId;
        this.stageId = stageId;
        this.executionId = executionId;
        this.batchArguments = Collections.unmodifiableMap(new LinkedHashMap<>(batchArguments));

        this.systemVariables = new VariableTable(VariableTable.RedefineStrategy.ERROR);
        this.systemVariables.defineVariables(toMap(userName, batchId, flowId, stageId, executionId));

        this.userVariables = new VariableTable(VariableTable.RedefineStrategy.IGNORE);
        this.userVariables.defineVariables(batchArguments);
        this.userVariables.defineVariables(toMap(userName, batchId, flowId, stageId, executionId));
    }

    /**
     * Creates a new instance.
     * @param userName the current user name (nullable)
     * @param batchId the current batch ID (nullable)
     * @param flowId the current flow ID (nullable)
     * @param stageId the current stage ID (nullable)
     * @param executionId the current execution ID (nullable)
     * @param serializedBatchArguments the serialized batch arguments (nullable)
     */
    public StageInfo(
            String userName,
            String batchId,
            String flowId,
            String stageId,
            String executionId,
            String serializedBatchArguments) {
        this(userName,
                batchId, flowId, stageId, executionId,
                deserializeVariableTable(serializedBatchArguments));
    }

    private static Map<String, String> toMap(
            String userName, String batchId, String flowId, String stageId, String executionId) {
        Map<String, String> results = new LinkedHashMap<>();
        put(results, StageConstants.VAR_USER, userName);
        put(results, StageConstants.VAR_BATCH_ID, batchId);
        put(results, StageConstants.VAR_FLOW_ID, flowId);
        put(results, StageConstants.VAR_STAGE_ID, stageId);
        put(results, StageConstants.VAR_EXECUTION_ID, executionId);
        return results;
    }

    private static void put(Map<String, String> map, String key, String value) {
        if (value == null) {
            return;
        }
        map.put(key, value);
    }

    /**
     * Returns the current user name.
     * @return the current user name
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Returns the current batch ID.
     * @return the batch ID
     */
    public String getBatchId() {
        return batchId;
    }

    /**
     * Returns the current flow ID.
     * @return the flow ID
     */
    public String getFlowId() {
        return flowId;
    }

    /**
     * Returns the current stage ID.
     * @return the stage ID
     */
    public String getStageId() {
        return stageId;
    }

    /**
     * Returns the current execution ID.
     * @return the execution ID
     */
    public String getExecutionId() {
        return executionId;
    }

    /**
     * Returns the current batch arguments.
     * @return the batch arguments
     */
    public Map<String, String> getBatchArguments() {
        return batchArguments;
    }

    /**
     * Resolves stage variables (<code>${&lt;variable-name&gt;}</code>) in the target string.
     * This never use {@link #getBatchArguments() batch arguments}.
     * @param string the target string
     * @return resolved string
     * @throws IllegalArgumentException if the string contains unknown variables
     */
    public String resolveVariables(String string) {
        return resolveSystemVariables(string);
    }

    /**
     * Resolves stage variables (<code>${&lt;variable-name&gt;}</code>) in the target string.
     * This never use {@link #getBatchArguments() batch arguments}.
     * @param string the target string
     * @return resolved string
     * @throws IllegalArgumentException if the string contains unknown variables
     * @see #resolveUserVariables(String)
     */
    public String resolveSystemVariables(String string) {
        return systemVariables.parse(string, true);
    }

    /**
     * Resolves user variables (<code>${&lt;variable-name&gt;}</code>) in the target string.
     * This also use {@link #getBatchArguments() batch arguments}.
     * @param string the target string
     * @return resolved string
     * @throws IllegalArgumentException if the string contains unknown variables
     * @see #resolveSystemVariables(String)
     */
    public String resolveUserVariables(String string) {
        return userVariables.parse(string, true);
    }

    /**
     * Returns a serialized string of this.
     * @return a serialized string
     * @see #deserialize(String)
     */
    public String serialize() {
        Map<String, String> map = new LinkedHashMap<>();
        map.putAll(toMap(userName, batchId, flowId, stageId, executionId));

        VariableTable batchArgs = new VariableTable();
        batchArgs.defineVariables(batchArguments);
        map.put(KEY_BATCH_ARGUMENTS, batchArgs.toSerialString());
        return serializeFromMap(map);
    }

    /**
     * Restores {@link StageInfo} from a {@link #serialize() serialized string}.
     * @param serialized serialized string
     * @return the deserialized object
     */
    public static StageInfo deserialize(String serialized) {
        Map<String, String> map = deserializeToMap(serialized);
        String userName = map.get(StageConstants.VAR_USER);
        String batchId = map.get(StageConstants.VAR_BATCH_ID);
        String flowId = map.get(StageConstants.VAR_FLOW_ID);
        String stageId = map.get(StageConstants.VAR_STAGE_ID);
        String executionId = map.get(StageConstants.VAR_EXECUTION_ID);
        String serialBatchArgs = map.get(KEY_BATCH_ARGUMENTS);
        Map<String, String> arguments = deserializeVariableTable(serialBatchArgs);
        return new StageInfo(userName, batchId, flowId, stageId, executionId, arguments);
    }

    private String serializeFromMap(Map<String, String> map) {
        StringBuilder buf = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (buf.length() != 0) {
                buf.append(SEPARATOR);
            }
            buf.append(serialize(entry.getKey()));
            buf.append(SEPARATOR);
            buf.append(serialize(entry.getValue()));
        }
        return buf.toString();
    }

    private String serialize(String value) {
        StringBuilder buf = new StringBuilder();
        for (char c : value.toCharArray()) {
            if (c == ESCAPE || c == SEPARATOR) {
                buf.append(ESCAPE);
            }
            buf.append(c);
        }
        return buf.toString();
    }

    private static Map<String, String> deserializeToMap(String serialized) {
        List<String> entries = deserializeToList(serialized);
        if (entries.size() % 2 == 1) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "failed to deserialize entries: {0}",
                    entries));
        }
        Map<String, String> results = new LinkedHashMap<>();
        for (int i = 0, n = entries.size(); i < n; i += 2) {
            String key = entries.get(i + 0);
            String value = entries.get(i + 1);
            if (results.containsKey(key)) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "failed to deserialize entries: {0}",
                        entries));
            }
            results.put(key, value);
        }
        return results;
    }

    private static List<String> deserializeToList(String serialized) {
        List<String> results = new ArrayList<>();
        StringBuilder buf = new StringBuilder();
        boolean sawEscape = false;
        for (char c : serialized.toCharArray()) {
            if (sawEscape) {
                sawEscape = false;
                buf.append(c);
                continue;
            } else if (c == ESCAPE) {
                sawEscape = true;
            } else if (c == SEPARATOR) {
                results.add(buf.toString());
                buf.setLength(0);
            } else {
                buf.append(c);
            }
        }
        results.add(buf.toString());
        return results;
    }

    private static Map<String, String> deserializeVariableTable(String serialized) {
        Map<String, String> arguments = new LinkedHashMap<>();
        if (serialized != null) {
            VariableTable table = new VariableTable();
            table.defineVariables(serialized);
            arguments.putAll(table.getVariables());
        }
        return arguments;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "'{'user={0}, batch={1}, flow={2}, stage={3}, execution={4}, arguments={5}'}'", //$NON-NLS-1$
                toString(userName),
                toString(batchId),
                toString(flowId),
                toString(stageId),
                toString(executionId),
                batchArguments);
    }

    private String toString(String id) {
        return Objects.toString(id, "N/A"); //$NON-NLS-1$
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Objects.hashCode(userName);
        result = prime * result + Objects.hashCode(batchId);
        result = prime * result + Objects.hashCode(executionId);
        result = prime * result + Objects.hashCode(flowId);
        result = prime * result + Objects.hashCode(stageId);
        result = prime * result + Objects.hashCode(batchArguments);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        StageInfo other = (StageInfo) obj;
        if (!Objects.equals(userName, other.userName)) {
            return false;
        }
        if (!Objects.equals(batchId, other.batchId)) {
            return false;
        }
        if (!Objects.equals(flowId, other.flowId)) {
            return false;
        }
        if (!Objects.equals(stageId, other.stageId)) {
            return false;
        }
        if (!Objects.equals(executionId, other.executionId)) {
            return false;
        }
        if (!Objects.equals(batchArguments, other.batchArguments)) {
            return false;
        }
        return true;
    }
}
