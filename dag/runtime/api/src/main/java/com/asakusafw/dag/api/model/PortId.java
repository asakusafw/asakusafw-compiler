/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.api.model;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Objects;

import com.asakusafw.dag.api.model.PortInfo.Direction;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents ID of vertex I/O ports.
 * @since 0.4.0
 */
public class PortId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final VertexId vertexId;

    private final String portName;

    private final PortInfo.Direction direction;

    /**
     * Creates a new instance.
     * @param vertexId the owner's vertex ID
     * @param name the target port name
     * @param direction the target port direction
     */
    public PortId(VertexId vertexId, String name, Direction direction) {
        Arguments.requireNonNull(vertexId);
        Arguments.requireNonNull(name);
        Arguments.requireNonNull(direction);
        this.vertexId = vertexId;
        this.portName = name;
        this.direction = direction;
    }

    /**
     * Returns the owner vertex ID.
     * @return the owner vertex ID
     */
    public VertexId getVertexId() {
        return vertexId;
    }

    /**
     * Returns the port name.
     * @return the port name
     */
    public String getName() {
        return portName;
    }

    /**
     * Returns the port direction.
     * @return the port direction
     */
    public PortInfo.Direction getDirection() {
        return direction;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Objects.hashCode(direction);
        result = prime * result + Objects.hashCode(portName);
        result = prime * result + Objects.hashCode(vertexId);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PortId other = (PortId) obj;
        if (direction != other.direction) {
            return false;
        }
        if (!portName.equals(other.portName)) {
            return false;
        }
        if (!vertexId.equals(other.vertexId)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Port[{2}]({0}:{1})", //$NON-NLS-1$
                vertexId.getName(),
                portName,
                direction);
    }
}
