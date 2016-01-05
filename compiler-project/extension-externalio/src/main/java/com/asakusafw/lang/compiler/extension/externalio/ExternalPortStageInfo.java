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
package com.asakusafw.lang.compiler.extension.externalio;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Represents a stage information which processes external ports.
 */
public class ExternalPortStageInfo {

    private final String moduleId;

    private final String batchId;

    private final String flowId;

    private final TaskReference.Phase phase;

    /**
     * Creates a new instance.
     * @param moduleId the external port processor's module ID
     * @param batchId the batch ID
     * @param flowId the flow ID
     * @param phase the execution phase
     */
    public ExternalPortStageInfo(String moduleId, String batchId, String flowId, TaskReference.Phase phase) {
        this.moduleId = moduleId;
        this.batchId = batchId;
        this.flowId = flowId;
        this.phase = phase;
    }

    /**
     * Returns the external port processor's module ID.
     * @return the module ID
     */
    public String getModuleId() {
        return moduleId;
    }

    /**
     * Returns the batch ID.
     * @return the batch ID
     */
    public String getBatchId() {
        return batchId;
    }

    /**
     * Returns the flow ID.
     * @return the flow ID
     */
    public String getFlowId() {
        return flowId;
    }

    /**
     * Returns the execution phase.
     * @return the execution phase
     */
    public TaskReference.Phase getPhase() {
        return phase;
    }

    /**
     * Returns the default stage ID.
     * @return the default stage ID
     */
    public String getStageId() {
        return Naming.getStageId(moduleId, phase);
    }

    /**
     * Returns the class name for this stage.
     * @param simpleName the class simple name
     * @return the class name
     */
    public ClassDescription getStageClass(String simpleName) {
        return Naming.getClass(moduleId, phase, simpleName);
    }

    /**
     * Returns the class name for this stage.
     * @param simpleNamePrefix the class simple name prefix
     * @param index the class index
     * @return the class name
     */
    public ClassDescription getStageClass(String simpleNamePrefix, int index) {
        return Naming.getClass(moduleId, phase, simpleNamePrefix, index);
    }
}
