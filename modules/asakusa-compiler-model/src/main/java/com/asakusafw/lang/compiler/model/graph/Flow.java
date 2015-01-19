package com.asakusafw.lang.compiler.model.graph;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Represents a flow.
 */
public interface Flow {

    /**
     * Returns the the original flow description class.
     * @return the flow description class
     */
    ClassDescription getDescriptionClass();

    /**
     * Returns the operator graph of this flow.
     * @return the operator graph
     */
    OperatorGraph getOperatorGraph();
}
