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
package com.asakusafw.dag.api.counter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.asakusafw.dag.api.counter.CounterGroup.Category;
import com.asakusafw.dag.api.counter.CounterGroup.Column;
import com.asakusafw.dag.api.counter.CounterGroup.Scope;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * A repository which can provide {@link CounterGroup}.
 * @since 0.4.0
 */
public interface CounterRepository {

    /**
     * The detached {@link CounterRepository}.
     */
    CounterRepository DETACHED = new CounterRepository() {
        @Override
        public <T extends CounterGroup> T get(Category<T> category, String vertexId, String itemId) {
            return category.newInstance();
        }
        @Override
        public Stream<Entry> stream() {
            return Stream.empty();
        }
    };

    /**
     * Returns a {@link CounterGroup}.
     * @param <T> the counter type
     * @param category the counter category
     * @param vertexId the context vertex ID, or {@code null} if the target counter group is on the graph scope
     * @param itemId the item ID for the target category
     * @return the related {@link CounterGroup}, never {@code null}
     * @see com.asakusafw.dag.api.counter.CounterGroup.Scope
     */
    <T extends CounterGroup> T get(Category<T> category, String vertexId, String itemId);

    /**
     * Returns a {@link CounterGroup} for the graph scope category.
     * @param <T> the counter type
     * @param category the counter category
     * @param itemId the item ID for the target category
     * @return the related {@link CounterGroup}, never {@code null}
     * @see com.asakusafw.dag.api.counter.CounterGroup.Scope
     */
    default <T extends CounterGroup> T get(Category<T> category, String itemId) {
        Arguments.requireNonNull(category);
        Arguments.requireNonNull(itemId);
        Arguments.require(category.getScope() == Scope.GRAPH);
        return get(category, null, itemId);
    }

    /**
     * Returns a stream which provides all entries in the repository.
     * @return the stream;
     */
    Stream<Entry> stream();

    /**
     * Merges two column-count map.
     * @param a the first map
     * @param b the second map
     * @return the merged map
     */
    static Map<Column, Long> merge(Map<Column, Long> a, Map<Column, Long> b) {
        Arguments.requireNonNull(a);
        Arguments.requireNonNull(b);
        Map<Column, Long> r = new LinkedHashMap<>(a);
        mergeInto(b, r);
        return r;
    }

    /**
     * Merges two column-count map.
     * @param source the source map
     * @param destination the destination map
     */
    static void mergeInto(Map<Column, Long> source, Map<Column, Long> destination) {
        Arguments.requireNonNull(source);
        Arguments.requireNonNull(destination);
        source.forEach((k, v) -> destination.merge(k, v, (x, y) -> x + y));
    }

    /**
     * Represents each entry in {@link CounterRepository}.
     * @since 0.4.0
     */
    interface Entry {

        /**
         * Returns the category.
         * @return the category
         */
        Category<?> getCategory();

        /**
         * Returns the scope.
         * @return the scope
         */
        default Scope getScope() {
            return getCategory().getScope();
        }

        /**
         * Returns the vertex ID.
         * @return the vertex ID, or {@code null} if the {@link #getScope() scope} is not vertex
         */
        String getVertexId();

        /**
         * Returns the item ID.
         * @return the item ID
         */
        String getItemId();

        /**
         * Returns the counters for each column.
         * @return the counters
         */
        default Map<Column, Long> getCounters() {
            Map<Column, Long> results = new LinkedHashMap<>();
            mergeInto(results);
            return results;
        }

        /**
         * Merges all counter columns into the target map.
         * @param target the target map
         */
        void mergeInto(Map<Column, Long> target);
    }
}
