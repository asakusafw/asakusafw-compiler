package com.asakusafw.lang.compiler.model.graph;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Represents a jobflow.
 */
public class Jobflow implements Flow {

    private final String flowId;

    private final ClassDescription descriptionClass;

    private final OperatorGraph operatorGraph;

    /**
     * Creates a new instance.
     * @param flowId the flow ID
     * @param descriptionClass the original flow description class name
     * @param operatorGraph the operator graph
     */
    public Jobflow(String flowId, ClassDescription descriptionClass, OperatorGraph operatorGraph) {
        this.flowId = flowId;
        this.descriptionClass = descriptionClass;
        this.operatorGraph = operatorGraph;
    }

    /**
     * Returns the flow ID.
     * @return the flow ID
     */
    public String getFlowId() {
        return flowId;
    }

    @Override
    public ClassDescription getDescriptionClass() {
        return descriptionClass;
    }

    @Override
    public OperatorGraph getOperatorGraph() {
        return operatorGraph;
    }
}
