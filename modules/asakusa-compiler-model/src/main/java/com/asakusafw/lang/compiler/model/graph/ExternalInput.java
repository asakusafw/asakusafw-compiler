package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;
import java.util.List;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents an external/flow input operator.
 */
public final class ExternalInput extends ExternalPort {

    /**
     * The port name used in {@link #getOperatorPort()}.
     */
    public static final String PORT_NAME = "port"; //$NON-NLS-1$

    private final String name;

    private final ClassDescription descriptionClass;

    private ExternalInput(String name, ClassDescription descriptionClass) {
        this.name = name;
        this.descriptionClass = descriptionClass;
    }

    @Override
    public ExternalInput copy() {
        return copyAttributesTo(new ExternalInput(name, descriptionClass));
    }

    /**
     * Creates a new builder.
     * Clients use {@link #newInstance(String, TypeDescription)} instead of this.
     * @param name the input name
     * @return the builder
     * @see #builder(String, ClassDescription)
     */
    public static Builder builder(String name) {
        return builder(name, null);
    }

    /**
     * Creates a new builder.
     * Clients use {@link #newInstance(String, ClassDescription, TypeDescription)} instead of this.
     * @param name the input name
     * @param descriptionClass the importer description class (nullable if the port is not external)
     * @return the builder
     */
    public static Builder builder(String name, ClassDescription descriptionClass) {
        return new Builder(new ExternalInput(name, descriptionClass));
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the input name
     * @param dataType the port type
     * @return the created instance
     * @see #newInstance(String, ClassDescription, TypeDescription)
     */
    public static ExternalInput newInstance(String name, TypeDescription dataType) {
        return builder(name)
                .output(PORT_NAME, dataType)
                .build();
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the input name
     * @param descriptionClass the importer description class (nullable if the port is not external)
     * @param dataType the port type
     * @return the created instance
     */
    public static ExternalInput newInstance(String name, ClassDescription descriptionClass, TypeDescription dataType) {
        return builder(name, descriptionClass)
                .output(PORT_NAME, dataType)
                .build();
    }

    @Override
    public final OperatorKind getOperatorKind() {
        return OperatorKind.INPUT;
    }

    @Override
    public PortKind getPortKind() {
        return PortKind.INPUT;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public OperatorOutput getOperatorPort() {
        List<OperatorOutput> ports = getOutputs();
        if (ports.size() != 1) {
            throw new IllegalStateException();
        }
        return ports.get(0);
    }

    @Override
    public ClassDescription getDescriptionClass() {
        return descriptionClass;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ExternalInput({0})", //$NON-NLS-1$
                name);
    }

    /**
     * A builder for {@link ExternalInput}.
     */
    public static final class Builder extends AbstractBuilder<ExternalInput, Builder> {

        Builder(ExternalInput owner) {
            super(owner);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }
    }
}
