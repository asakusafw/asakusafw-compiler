/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.analyzer.util;

import java.text.MessageFormat;

/**
 * Represents a folding properties.
 */
public class PropertyFolding {

    private final PropertyMapping mapping;

    private final Aggregation aggregation;

    /**
     * Creates a new instance.
     * @param mapping the mapping information
     * @param aggregation the aggregation strategy
     */
    public PropertyFolding(PropertyMapping mapping, Aggregation aggregation) {
        this.mapping = mapping;
        this.aggregation = aggregation;
    }

    /**
     * Returns the property mapping information.
     * @return the mapping information
     */
    public PropertyMapping getMapping() {
        return mapping;
    }

    /**
     * Returns the aggregation strategy.
     * @return the aggregation strategy
     */
    public Aggregation getAggregation() {
        return aggregation;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "PropertyFolding(mapping={0}, aggregation={1})", //$NON-NLS-1$
                mapping,
                aggregation);
    }

    /**
     * Aggregation strategy.
     */
    public enum Aggregation {

        /**
         * Any of element in bag.
         */
        ANY,

        /**
         * Sum of element in bag.
         */
        SUM,

        /**
         * Cardinality of element in bag.
         */
        COUNT,

        /**
         * Maximum of element in bag.
         */
        MAX,

        /**
         * Minimum of element in bag.
         */
        MIN,
    }
}
