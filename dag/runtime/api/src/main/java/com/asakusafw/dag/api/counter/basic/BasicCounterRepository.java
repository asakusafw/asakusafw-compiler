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
package com.asakusafw.dag.api.counter.basic;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import com.asakusafw.dag.api.counter.CounterGroup;
import com.asakusafw.dag.api.counter.CounterGroup.Category;
import com.asakusafw.dag.api.counter.CounterGroup.Column;
import com.asakusafw.dag.api.counter.CounterGroup.Scope;
import com.asakusafw.dag.api.counter.CounterRepository;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * A basic implementation of {@link CounterRepository}.
 * @since 0.4.0
 */
public class BasicCounterRepository implements CounterRepository {

    private final Map<Key, CounterGroup> entries = new LinkedHashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public <T extends CounterGroup> T get(Category<T> category, String vertexId, String itemId) {
        Arguments.requireNonNull(category);
        Arguments.requireNonNull(itemId);
        Scope scope = category.getScope();
        Arguments.require(scope == Scope.GRAPH || vertexId != null);
        Key key = new Key(category, scope == Scope.GRAPH ? null : vertexId, itemId);
        synchronized (entries) {
            return (T) entries.computeIfAbsent(key, k -> category.newInstance());
        }
    }

    @Override
    public Stream<Entry> stream() {
        synchronized (entries) {
            List<Entry> source = new ArrayList<>(entries.size());
            entries.forEach((k, v) -> source.add(new BasicEntry(k, v)));
            return source.stream();
        }
    }

    private static final class Key {

        final Category<?> category;

        final String vertexId;

        final String itemId;

        Key(Category<?> category, String vertexId, String itemId) {
            this.category = category;
            this.vertexId = vertexId;
            this.itemId = itemId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hashCode(category);
            result = prime * result + Objects.hashCode(itemId);
            result = prime * result + Objects.hashCode(vertexId);
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
            Key other = (Key) obj;
            if (!Objects.equals(category, other.category)) {
                return false;
            }
            if (!Objects.equals(itemId, other.itemId)) {
                return false;
            }
            if (!Objects.equals(vertexId, other.vertexId)) {
                return false;
            }
            return true;
        }
    }

    private static final class BasicEntry implements Entry {

        final Key key;

        final CounterGroup group;

        BasicEntry(Key key, CounterGroup group) {
            this.key = key;
            this.group = group;
        }

        @Override
        public Category<?> getCategory() {
            return key.category;
        }

        @Override
        public String getVertexId() {
            return key.vertexId;
        }

        @Override
        public String getItemId() {
            return key.itemId;
        }

        @Override
        public void mergeInto(Map<Column, Long> target) {
            for (Column c : key.category.getColumns()) {
                target.merge(c, group.getCount(c), (x, y) -> x + y);
            }
        }
    }
}
