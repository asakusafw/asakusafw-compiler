package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;
import java.util.List;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents an external/flow output operator.
 */
public final class ExternalOutput extends ExternalPort {

    /**
     * The port name used in {@link #getOperatorPort()}.
     */
    public static final String PORT_NAME = "port"; //$NON-NLS-1$

    private final String name;

    private final ClassDescription descriptionClass;

    private ExternalOutput(String name, ClassDescription descriptionClass) {
        this.name = name;
        this.descriptionClass = descriptionClass;
    }

    @Override
    public ExternalOutput copy() {
        return copyAttributesTo(new ExternalOutput(name, descriptionClass));
    }

    /**
     * Creates a new builder.
     * Usually, clients use {@link #newInstance(String, TypeDescription, OperatorOutput...)} instead.
     * @param name the output name
     * @return the builder
     * @see #builder(String, ClassDescription)
     */
    public static Builder builder(String name) {
        return builder(name, null);
    }

    /**
     * Creates a new builder.
     * Usually, clients use {@link #newInstance(String, ClassDescription, TypeDescription, OperatorOutput...)} instead.
     * @param name the output name
     * @param descriptionClass the exporter description class (nullable if the port is not external)
     * @return the builder
     */
    public static Builder builder(String name, ClassDescription descriptionClass) {
        return new Builder(new ExternalOutput(name, descriptionClass));
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param dataType the port type
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     * @see #newInstance(String, ClassDescription, TypeDescription, OperatorOutput...)
     */
    public static ExternalOutput newInstance(
            String name,
            TypeDescription dataType,
            OperatorOutput... upstreams) {
        return newInstance(name, null, dataType, upstreams);
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param upstream the mandatory upstream port to connect to the created operator
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     * @see #newInstance(String, ClassDescription, TypeDescription, OperatorOutput...)
     */
    public static ExternalOutput newInstance(
            String name,
            OperatorOutput upstream,
            OperatorOutput... upstreams) {
        return newInstance(name, null, upstream, upstreams);
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param descriptionClass the exporter description class (nullable if the port is not external)
     * @param dataType the port type
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     */
    public static ExternalOutput newInstance(
            String name, ClassDescription descriptionClass,
            TypeDescription dataType,
            OperatorOutput... upstreams) {
        return builder(name, descriptionClass)
                .input(PORT_NAME, dataType, upstreams)
                .build();
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param descriptionClass the exporter description class (nullable if the port is not external)
     * @param upstream the mandatory upstream port to connect to the created operator
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     */
    public static ExternalOutput newInstance(
            String name, ClassDescription descriptionClass,
            OperatorOutput upstream,
            OperatorOutput... upstreams) {
        return builder(name, descriptionClass)
                .input(PORT_NAME, upstream, upstreams)
                .build();
    }

    @Override
    public final OperatorKind getOperatorKind() {
        return OperatorKind.OUTPUT;
    }

    @Override
    public PortKind getPortKind() {
        return PortKind.OUTPUT;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public OperatorInput getOperatorPort() {
        List<OperatorInput> ports = getInputs();
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
                "ExternalOutput({0})", //$NON-NLS-1$
                name);
    }

    /**
     * A builder for {@link ExternalOutput}.
     */
    public static final class Builder extends AbstractBuilder<ExternalOutput, Builder> {

        Builder(ExternalOutput owner) {
            super(owner);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }

        @Override
        public ExternalOutput build() {
            if (getOwner().getInputs().size() != 1 || getOwner().getOutputs().size() > 1) {
                throw new IllegalStateException();
            }
            return super.build();
        }
    }
}
