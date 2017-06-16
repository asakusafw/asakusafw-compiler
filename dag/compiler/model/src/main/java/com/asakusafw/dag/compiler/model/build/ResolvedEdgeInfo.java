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
package com.asakusafw.dag.compiler.model.build;

import com.asakusafw.dag.api.model.EdgeDescriptor;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;

/**
 * Represents a resolved edge.
 * @since 0.4.2
 */
public class ResolvedEdgeInfo {

    private final EdgeDescriptor descriptor;

    private final Movement movement;

    private final TypeDescription dataType;

    private final Group group;

    /**
     * Creates a new instance.
     * @param descriptor the actual descriptor
     * @param movement the movement type
     * @param dataType the data type
     * @param group the grouping information
     */
    public ResolvedEdgeInfo(
            EdgeDescriptor descriptor, Movement movement, TypeDescription dataType, Group group) {
        this.descriptor = descriptor;
        this.movement = movement;
        this.dataType = dataType;
        this.group = group;
    }

    /**
     * Returns the platform dependent descriptor.
     * @return the descriptor
     */
    public EdgeDescriptor getDescriptor() {
        return descriptor;
    }

    /**
     * Returns the data exchange type.
     * @return the data exchange type
     */
    public Movement getMovement() {
        return movement;
    }

    /**
     * Returns the data type.
     * @return the data type, or {@code null} if it is not defined
     */
    public TypeDescription getDataType() {
        return dataType;
    }

    /**
     * Returns the grouping information.
     * @return the grouping information, or {@code null} if it is not defined
     */
    public Group getGroup() {
        return group;
    }

    /**
     * Represents data exchange operation type.
     * @since 0.4.2
     */
    public enum Movement {

        /**
         * Distributes nothing to successors.
         */
        NOTHING,

        /**
         * Distributes each fragment to successors.
         */
        ONE_TO_ONE,

        /**
         * Distributes all fragments into each successor.
         */
        BROADCAST,

        /**
         * Builds a sequence of sorted record groups from the fragments, and distributes them to successors.
         */
        SCATTER_GATHER,

        /**
         * Aggregates record groups and distributes them to successors.
         */
        AGGREGATE,
    }
}
