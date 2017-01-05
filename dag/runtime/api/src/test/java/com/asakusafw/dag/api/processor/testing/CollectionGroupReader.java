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
package com.asakusafw.dag.api.processor.testing;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;

import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * A {@link GroupReader} implementation which reads groups from {@link SortedMap} object.
 */
public class CollectionGroupReader implements GroupReader {

    private final Iterator<? extends Map.Entry<?, ? extends Collection<?>>> groups;

    private GroupInfo key;

    private CollectionObjectReader current;

    private final Comparator<Object> comparator;

    /**
     * Creates a new instance.
     * @param map the group map
     */
    @SuppressWarnings("unchecked")
    public CollectionGroupReader(SortedMap<?, ? extends Collection<?>> map) {
        this.groups = map.entrySet().iterator();
        this.comparator = Optionals.of((Comparator<Object>) map.comparator())
                .orElse((a, b) -> ((Comparable<Object>) a).compareTo(b));
    }

    @Override
    public boolean nextGroup() {
        if (groups.hasNext()) {
            Map.Entry<?, ? extends Collection<?>> entry = groups.next();
            key = new GroupInfoWrapper(comparator, entry.getKey());
            current = new CollectionObjectReader(entry.getValue());
            return true;
        } else {
            key = null;
            current = null;
            return false;
        }
    }

    @Override
    public GroupInfo getGroup() {
        Arguments.requireNonNull(key);
        return key;
    }

    @Override
    public boolean nextObject() {
        Arguments.requireNonNull(current);
        return current.nextObject();
    }

    @Override
    public Object getObject() {
        Arguments.requireNonNull(current);
        return current.getObject();
    }

    private static class GroupInfoWrapper implements GroupInfo {

        private final Comparator<Object> comparator;

        private final Object value;

        GroupInfoWrapper(Comparator<Object> comparator, Object value) {
            assert comparator != null;
            this.comparator = comparator;
            this.value = value;
        }

        @Override
        public Object getValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public int compareTo(GroupInfo o) {
            return comparator.compare(value, ((GroupInfoWrapper) o).value);
        }

        @Override
        public String toString() {
            return Objects.toString(value);
        }
    }
}
