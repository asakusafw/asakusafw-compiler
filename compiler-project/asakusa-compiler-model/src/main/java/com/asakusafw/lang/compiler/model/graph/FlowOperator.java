package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Represents a nested flow operator.
 */
public final class FlowOperator extends Operator implements Flow {

    private final ClassDescription descriptionClass;

    private final OperatorGraph operatorGraph;

    private FlowOperator(ClassDescription descriptionClass, OperatorGraph operatorGraph) {
        this.descriptionClass = descriptionClass;
        this.operatorGraph = operatorGraph;
    }

    @Override
    public FlowOperator copy() {
        return copyAttributesTo(new FlowOperator(descriptionClass, operatorGraph.copy()));
    }

    /**
     * Creates a new builder.
     * @param descriptionClass the flow description class name
     * @param operatorGraph the operator graph
     * @return the builder
     */
    public static Builder builder(ClassDescription descriptionClass, OperatorGraph operatorGraph) {
        return new Builder(new FlowOperator(descriptionClass, operatorGraph));
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.FLOW;
    }

    @Override
    public ClassDescription getDescriptionClass() {
        return descriptionClass;
    }

    @Override
    public OperatorGraph getOperatorGraph() {
        return operatorGraph;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "FlowOperator({0})", //$NON-NLS-1$
                getDescriptionClass().getSimpleName());
    }

    /**
     * A builder for {@link FlowOperator}.
     */
    public static final class Builder extends AbstractBuilder<FlowOperator, Builder> {

        Builder(FlowOperator owner) {
            super(owner);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }
    }
}
