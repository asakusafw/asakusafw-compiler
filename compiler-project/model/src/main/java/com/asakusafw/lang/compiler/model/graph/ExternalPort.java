/**
 * Copyright 2011-2016 Asakusa Framework Team.
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

import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;

/**
 * Represents an external/flow I/O port.
 */
public abstract class ExternalPort extends Operator {

    /**
     * Returns the port kind.
     * @return the port kind
     */
    public abstract PortKind getPortKind();

    /**
     * Returns the port name.
     * Each port name is identical in input ports or output ports in the same {@link OperatorGraph}.
     * @return the port name
     */
    public abstract String getName();

    /**
     * Returns the operator port.
     * @return the operator port
     */
    public abstract OperatorPort getOperatorPort();

    /**
     * Returns the data type on this port.
     * @return the data type
     */
    public TypeDescription getDataType() {
        return getOperatorPort().getDataType();
    }

    /**
     * Returns whether this port is external or not.
     * @return {@code true} if this is external, otherwise {@code false}
     */
    public boolean isExternal() {
        return getInfo() != null;
    }

    /**
     * Returns structural information of this external I/O port.
     * @return the structural information, or {@code null} if this port is not external
     */
    public abstract ExternalPortInfo getInfo();

    /**
     * Represents a kind of port.
     */
    public static enum PortKind {

        /**
         * input port.
         * @see ExternalInput
         */
        INPUT,

        /**
         * output port.
         * @see ExternalOutput
         */
        OUTPUT,
    }
}
