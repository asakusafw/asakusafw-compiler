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
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;

/**
 * Represents an external/flow input operator.
 */
public final class ExternalInput extends ExternalPort {

    /**
     * The port name used in {@link #getOperatorPort()}.
     */
    public static final String PORT_NAME = "port"; //$NON-NLS-1$

    private final String name;

    private final ExternalInputInfo.Basic info;

    private ExternalInput(String name, ExternalInputInfo info) {
        this.name = name;
        this.info = info == null ? null : new ExternalInputInfo.Basic(info);
    }

    @Override
    public ExternalInput copy() {
        return copyAttributesTo(new ExternalInput(name, info));
    }

    /**
     * Creates a new builder.
     * Clients use {@link #newInstance(String, TypeDescription)} instead of this.
     * @param name the input name
     * @return the builder
     * @see #builder(String, ExternalInputInfo)
     */
    public static Builder builder(String name) {
        return builder(name, null);
    }

    /**
     * Creates a new builder.
     * Clients use {@link #newInstance(String, ExternalInputInfo)} instead of this.
     * @param name the input name
     * @param info the structural importer information (nullable if the port is not external)
     * @return the builder
     */
    public static Builder builder(String name, ExternalInputInfo info) {
        return new Builder(new ExternalInput(name, info));
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the input name
     * @param dataType the port type
     * @return the created instance
     * @see #newInstance
     */
    public static ExternalInput newInstance(String name, TypeDescription dataType) {
        return builder(name)
                .output(PORT_NAME, dataType)
                .build();
    }

    /**
     * Creates a new instance with default {@link #getOperatorPort() operator port}.
     * @param name the input name
     * @param info the structural importer information (nullable if the port is not external)
     * @return the created instance
     */
    public static ExternalInput newInstance(String name, ExternalInputInfo info) {
        return builder(name, info)
                .output(PORT_NAME, info.getDataModelClass())
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
    public ExternalInputInfo getInfo() {
        return info;
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

        @Override
        public ExternalInput build() {
            if (getOwner().getInputs().size() > 1 || getOwner().getOutputs().size() != 1) {
                throw new IllegalStateException();
            }
            return super.build();
        }
    }
}
