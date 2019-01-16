/**
 * Copyright 2011-2019 Asakusa Framework Team.
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

import com.asakusafw.lang.utils.common.Arguments;

/**
 * Describes I/O ports of vertices.
 * @since 0.4.0
 */
public class PortInfo implements Serializable {

    private static final long serialVersionUID = 2L;

    private final PortId id;

    private final String tag;

    /**
     * Creates a new instance.
     * @param id the ID of this port
     */
    public PortInfo(PortId id) {
        this(id, null);
    }

    /**
     * Creates a new instance.
     * @param id the ID of this port
     * @param tag the port tag (nullable)
     */
    public PortInfo(PortId id, String tag) {
        Arguments.requireNonNull(id);
        this.id = id;
        this.tag = tag;
    }

    /**
     * Returns the ID of this port.
     * @return the port ID
     */
    public PortId getId() {
        return id;
    }

    /**
     * Returns the port name.
     * @return the port name
     */
    public String getName() {
        return getId().getName();
    }

    /**
     * Returns the port tag.
     * @return the port tag, or {@code null} if it is not defined
     */
    public String getTag() {
        return tag;
    }

    /**
     * Returns the port direction.
     * @return the port direction
     */
    public Direction getDirection() {
        return getId().getDirection();
    }

    /**
     * Represents port direction kinds.
     */
    public enum Direction {

        /**
         * Upstream ports.
         */
        INPUT,

        /**
         * Downstream ports.
         */
        OUTPUT,
    }
}
