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
