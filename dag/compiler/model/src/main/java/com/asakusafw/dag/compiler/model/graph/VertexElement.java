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
package com.asakusafw.dag.compiler.model.graph;

import java.util.List;

import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents an element in vertex.
 * @since 0.4.0
 */
public interface VertexElement {

    /**
     * Returns the element kind.
     * @return the element kind
     */
    ElementKind getElementKind();

    /**
     * Returns the runtime type of this element.
     * @return the runtime type
     */
    TypeDescription getRuntimeType();

    /**
     * Returns the dependency elements.
     * @return the dependency elements
     */
    List<? extends VertexElement> getDependencies();

    /**
     * Represents a kind of {@link VertexElement}.
     * @since 0.4.0
     * @version 0.4.1
     */
    enum ElementKind {

        /**
         * Represents the running context.
         */
        CONTEXT,

        /**
         * Represents a value.
         */
        VALUE,

        /**
         * Represents vertex data table.
         */
        DATA_TABLE,

        /**
         * Represents a vertex main input.
         * Each vertex must be up to one main input.
         */
        INPUT,

        /**
         * Represents a vertex output.
         */
        OUTPUT,

        /**
         * Represents an operator like element.
         */
        OPERATOR,

        /**
         * Represents an aggregation.
         */
        AGGREGATE,

        /**
         * Represents an empty data table.
         * @since 0.4.1
         */
        EMPTY_DATA_TABLE,
    }
}
