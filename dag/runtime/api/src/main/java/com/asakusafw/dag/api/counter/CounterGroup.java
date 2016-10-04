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
package com.asakusafw.dag.api.counter;

import java.util.List;

/**
 * Represents a group of counter.
 * @since 0.4.0
 */
@FunctionalInterface
public interface CounterGroup {

    /**
     * Returns the actual count for the target column.
     * @param column the target column
     * @return the actual count
     */
    long getCount(Column column);

    /**
     * An abstract super interface of counter elements.
     * @since 0.4.0
     */
    interface Element {

        /**
         * Returns the description of this element.
         * @return the description of this element
         */
        String getDescription();

        /**
         * Returns the index text of this element.
         * @return the index text
         */
        default String getIndexText() {
            return String.format("?.%s", getDescription()); //$NON-NLS-1$
        }
    }

    /**
     * Represents a column meta-data of {@link CounterGroup}.
     * @since 0.4.0
     */
    interface Column extends Element {
        // no special methods
    }

    /**
     * Represents a category of {@link CounterGroup}.
     * @param <T> the type of member {@link CounterGroup}
     * @since 0.4.0
     */
    interface Category<T extends CounterGroup> extends Element {

        /**
         * Returns the scope of the target {@link CounterGroup}.
         * @return the scope
         */
        Scope getScope();

        /**
         * Returns the set available columns in the target {@link CounterGroup}.
         * @return the available columns
         */
        List<Column> getColumns();

        /**
         * Creates a new {@link CounterGroup} which is member in this category.
         * @return the created {@link CounterGroup}
         */
        T newInstance();
    }

    /**
     * Represents a scope of {@link CounterGroup}.
     * @since 0.4.0
     */
    enum Scope {

        /**
         * Graph scoped {@link CounterGroup}.
         */
        GRAPH,

        /**
         * Vertex scoped {@link CounterGroup}.
         */
        VERTEX,
    }
}
