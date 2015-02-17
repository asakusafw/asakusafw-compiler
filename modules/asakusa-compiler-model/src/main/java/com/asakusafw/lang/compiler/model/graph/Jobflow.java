package com.asakusafw.lang.compiler.model.graph;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;

/**
 * Represents a jobflow.
 */
public class Jobflow extends JobflowInfo.Basic implements Flow {

    private final OperatorGraph operatorGraph;

    /**
     * Creates a new instance.
     * @param flowId the flow ID
     * @param descriptionClass the original flow description class name
     * @param operatorGraph the operator graph
     */
    public Jobflow(String flowId, ClassDescription descriptionClass, OperatorGraph operatorGraph) {
        super(flowId, descriptionClass);
        this.operatorGraph = operatorGraph;
    }

    /**
     * Creates a new instance.
     * @param info the structural information of this jobflow
     * @param operatorGraph the operator graph
     */
    public Jobflow(JobflowInfo info, OperatorGraph operatorGraph) {
        this(info.getFlowId(), info.getDescriptionClass(), operatorGraph);
    }

    @Override
    public OperatorGraph getOperatorGraph() {
        return operatorGraph;
    }
}
