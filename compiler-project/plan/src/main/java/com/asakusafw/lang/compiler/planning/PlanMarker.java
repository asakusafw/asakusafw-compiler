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
package com.asakusafw.lang.compiler.planning;

import com.asakusafw.lang.compiler.model.graph.OperatorAttribute;

/**
 * Represents a planning hint.
 */
public enum PlanMarker implements OperatorAttribute {

    /**
     * beginning of the operator graph.
     */
    BEGIN,

    /**
     * ending of the operator graph.
     */
    END,

    /**
     * required checkpoint operation on the marked location.
     */
    CHECKPOINT,

    /**
     * gathering operation is on the next of marked location.
     */
    GATHER,

    /**
     * broadcast operation is on the next of marked location.
     */
    BROADCAST,
    ;

    @Override
    public PlanMarker copy() {
        return this;
    }
}
