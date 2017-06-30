/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.api.model.basic;

import java.text.MessageFormat;
import java.util.Objects;

import com.asakusafw.dag.api.common.DataComparator;
import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.api.common.SupplierInfo;
import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.api.model.EdgeDescriptor;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * A basic implementation of {@link EdgeDescriptor}.
 * @since 0.4.2
 */
public class BasicEdgeDescriptor implements EdgeDescriptor {

    private static final long serialVersionUID = 1L;

    private final Movement movement;

    private final SupplierInfo serde;

    private final SupplierInfo comparator;

    /**
     * Creates a new instance.
     * @param movement the movement type
     * @param serde information of supplier which provides
     *     either {@link ValueSerDe} or {@link KeyValueSerDe} (nullable)
     * @param comparator information of supplier which provides {@link DataComparator} (nullable)
     */
    public BasicEdgeDescriptor(Movement movement, SupplierInfo serde, SupplierInfo comparator) {
        Arguments.requireNonNull(movement);
        switch (movement) {
        case ONE_TO_ONE:
        case BROADCAST:
            Arguments.require(serde != null);
            Arguments.require(comparator == null);
            break;
        case SCATTER_GATHER:
            Arguments.require(serde != null);
            break;
        case NOTHING:
            Arguments.require(serde == null);
            Arguments.require(comparator == null);
            break;
        default:
            throw new AssertionError();
        }
        this.movement = movement;
        this.serde = serde;
        this.comparator = comparator;
    }

    /**
     * Returns the movement type.
     * @return the movement type
     */
    public Movement getMovement() {
        return movement;
    }

    /**
     * Returns the information of supplier which provides either {@link ValueSerDe} or {@link KeyValueSerDe}.
     * @return the ser/de supplier information, or {@code null} if it is not defined
     */
    public SupplierInfo getSerDe() {
        return serde;
    }

    /**
     * Returns the information of supplier which provides {@link DataComparator}.
     * @return the comparator supplier information, or {@code null} if it is not defined
     */
    public SupplierInfo getComparator() {
        return comparator;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Objects.hashCode(movement);
        result = prime * result + Objects.hashCode(serde);
        result = prime * result + Objects.hashCode(comparator);
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
        BasicEdgeDescriptor other = (BasicEdgeDescriptor) obj;
        if (movement != other.movement) {
            return false;
        }
        if (!Objects.equals(serde, other.serde)) {
            return false;
        }
        if (!Objects.equals(comparator, other.comparator)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Edge({0})", //$NON-NLS-1$
                movement);
    }

    /**
     * Represents data exchange operation type.
     * @since 0.4.2
     */
    public enum Movement {

        /**
         * Distributes nothing to successors.
         */
        NOTHING(PortType.VOID),

        /**
         * Distributes each fragment to successors.
         */
        ONE_TO_ONE(PortType.VALUE),

        /**
         * Distributes all fragments into each successor.
         */
        BROADCAST(PortType.VALUE),

        /**
         * Builds a sequence of sorted record groups from the fragments, and distributes them to successors.
         */
        SCATTER_GATHER(PortType.KEY_VALUE),
        ;

        private final PortType portType;

        Movement(PortType portType) {
            this.portType = portType;
        }

        /**
         * Returns the port type.
         * @return the port type
         */
        public PortType getPortType() {
            return portType;
        }
    }

    /**
     * Represents data type of each I/O port.
     * @since 0.4.2
     */
    public enum PortType {

        /**
         * Nothing.
         */
        VOID,

        /**
         * Values only.
         */
        VALUE,

        /**
         * Key value pairs.
         */
        KEY_VALUE,
    }
}
