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
package com.asakusafw.lang.compiler.model.graph;

import java.text.MessageFormat;
import java.util.List;

import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Represents an external/flow output operator.
 * @since 0.1.0
 * @version 0.3.0
 */
public final class ExternalOutput extends ExternalPort {

    /**
     * The port name used in {@link #getOperatorPort()}.
     */
    public static final String PORT_NAME = "port"; //$NON-NLS-1$

    private final String name;

    private final ExternalOutputInfo.Basic info;

    private ExternalOutput(String name, ExternalOutputInfo info) {
        this.name = name;
        this.info = info == null ? null : new ExternalOutputInfo.Basic(info);
    }

    @Override
    public ExternalOutput copy() {
        return copyAttributesTo(new ExternalOutput(name, info));
    }

    /**
     * Creates a new builder.
     * Usually, clients use {@link #newInstance(String, TypeDescription, OperatorOutput...)} instead.
     * @param name the output name
     * @return the builder
     * @see #builder(String, ExternalOutputInfo)
     */
    public static Builder builder(String name) {
        return builder(name, null);
    }

    /**
     * Creates a new builder.
     * Usually, clients use {@link #newInstance(String, ExternalOutputInfo, OperatorOutput...)} instead.
     * @param name the output name
     * @param info the structural exporter information (nullable if the port is not external)
     * @return the builder
     */
    public static Builder builder(String name, ExternalOutputInfo info) {
        return new Builder(new ExternalOutput(name, info));
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param dataType the port type
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     * @see #newInstance(String, ExternalOutputInfo, OperatorOutput...)
     */
    public static ExternalOutput newInstance(String name, TypeDescription dataType, OperatorOutput... upstreams) {
        return builder(name, null)
                .input(PORT_NAME, dataType, upstreams)
                .build();
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param upstream the mandatory upstream port to connect to the created operator
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     * @see #newInstance(String, ExternalOutputInfo, OperatorOutput, OperatorOutput...)
     */
    public static ExternalOutput newInstance(String name, OperatorOutput upstream, OperatorOutput... upstreams) {
        return newInstance(name, null, upstream, upstreams);
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param info the structural exporter information (nullable if the port is not external)
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     */
    public static ExternalOutput newInstance(String name, ExternalOutputInfo info, OperatorOutput... upstreams) {
        return builder(name, info)
                .input(PORT_NAME, info.getDataModelClass(), upstreams)
                .build();
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param info the structural exporter information (nullable if the port is not external)
     * @param upstream the mandatory upstream port to connect to the created operator
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     */
    public static ExternalOutput newInstance(
            String name,
            ExternalOutputInfo info,
            OperatorOutput upstream,
            OperatorOutput... upstreams) {
        return builder(name, info)
                .input(PORT_NAME, upstream, upstreams)
                .build();
    }

    /**
     * Creates a new builder with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param dataType the port type
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     * @see #newWithAttributes(String, ExternalOutputInfo, OperatorOutput...)
     * @since 0.3.0
     */
    public static AttributeBuilder newWithAttributes(
            String name, TypeDescription dataType, OperatorOutput... upstreams) {
        return new AttributeBuilder(newInstance(name, dataType, upstreams));
    }

    /**
     * Creates a new builder with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param upstream the mandatory upstream port to connect to the created operator
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     * @see #newWithAttributes(String, ExternalOutputInfo, OperatorOutput, OperatorOutput...)
     * @since 0.3.0
     */
    public static AttributeBuilder newWithAttributes(
            String name, OperatorOutput upstream, OperatorOutput... upstreams) {
        return new AttributeBuilder(newInstance(name, upstream, upstreams));
    }

    /**
     * Creates a new builder with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param info the structural exporter information (nullable if the port is not external)
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     * @since 0.3.0
     */
    public static AttributeBuilder newWithAttributes(
            String name, ExternalOutputInfo info, OperatorOutput... upstreams) {
        return new AttributeBuilder(newInstance(name, info, upstreams));
    }

    /**
     * Creates a new builder with default {@link #getOperatorPort() operator port}.
     * @param name the output name
     * @param info the structural exporter information (nullable if the port is not external)
     * @param upstream the mandatory upstream port to connect to the created operator
     * @param upstreams the optional upstream ports to connect to the created operator
     * @return the created instance
     * @since 0.3.0
     */
    public static AttributeBuilder newWithAttributes(
            String name,
            ExternalOutputInfo info,
            OperatorOutput upstream,
            OperatorOutput... upstreams) {
        return new AttributeBuilder(newInstance(name, info, upstream, upstreams));
    }

    @Override
    public OperatorKind getOperatorKind() {
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
    public ExternalOutputInfo getInfo() {
        return info;
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

    /**
     * A builder for declaring attributes.
     * @since 0.3.0
     */
    public static final class AttributeBuilder extends BuilderBase<ExternalOutput, AttributeBuilder> {

        AttributeBuilder(ExternalOutput owner) {
            super(owner);
        }

        @Override
        protected AttributeBuilder getSelf() {
            return this;
        }
    }
}
