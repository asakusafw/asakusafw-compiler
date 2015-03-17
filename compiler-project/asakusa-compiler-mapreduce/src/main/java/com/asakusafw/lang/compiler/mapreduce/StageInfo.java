/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.mapreduce;

/**
 * Represents a common stage information.
 */
public class StageInfo {

    private final String batchId;

    private final String flowId;

    private final String stageId;

    /**
     * Creates a new instance.
     * @param batchId the batch ID
     * @param flowId the flow ID
     * @param stageId the stage ID
     */
    public StageInfo(String batchId, String flowId, String stageId) {
        this.batchId = batchId;
        this.flowId = flowId;
        this.stageId = stageId;
    }

    /**
     * Returns the target batch ID.
     * @return the target batch ID
     */
    public String getBatchId() {
        return batchId;
    }

    /**
     * Returns the target flow ID.
     * @return the target flow ID
     */
    public String getFlowId() {
        return flowId;
    }

    /**
     * Returns the target stage ID.
     * @return the target stage ID
     */
    public String getStageId() {
        return stageId;
    }
}
