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

import java.util.Collection;

import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents an I/O port of operator.
 */
public interface OperatorPort extends OperatorProperty {

    /**
     * Returns the declaring operator.
     * @return the declaring operator
     */
    Operator getOwner();

    /**
     * Returns the port name.
     * @return the port name
     */
    @Override
    String getName();

    /**
     * Returns the data type on this port.
     * @return the data type
     */
    TypeDescription getDataType();

    /**
     * Disconnects from all opposite ports.
     */
    void disconnectAll();

    /**
     * Returns whether this port has at least one opposite or not.
     * @return {@code true} if this port has any opposites
     */
    boolean hasOpposites();

    /**
     * Returns the opposite ports.
     * @return the opposite ports
     */
    Collection<? extends OperatorPort> getOpposites();
}
