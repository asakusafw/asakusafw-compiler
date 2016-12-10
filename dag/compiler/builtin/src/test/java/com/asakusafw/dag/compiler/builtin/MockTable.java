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
package com.asakusafw.dag.compiler.builtin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Mock {@link DataTable}.
 * @param <T> the element type
 * @since WIP
 */
public class MockTable<T> implements DataTable<T> {

    private final Map<Object, List<T>> map = new HashMap<>();

    private final Function<? super T, ?> keys;

    private final Comparator<? super T> comparator;

    private boolean changed;

    /**
     * Creates a new instance.
     * @param keys key term extractor
     */
    public MockTable(Function<? super T, ?> keys) {
        this(keys, null);
    }

    /**
     * Creates a new instance.
     * @param keys key term extractor
     * @param comparator element comparator (nullable)
     */
    public MockTable(Function<? super T, ?> keys, Comparator<? super T> comparator) {
        this.keys = keys;
        this.comparator = comparator;
    }

    /**
     * Adds an element.
     * @param object the element
     * @return this
     */
    public MockTable<T> add(T object) {
        map.computeIfAbsent(keys.apply(object), k -> new ArrayList<>()).add(object);
        changed = true;
        return this;
    }

    @Override
    public List<T> find(Object... elements) {
        Arguments.require(elements.length == 1);
        if (changed) {
            if (comparator != null) {
                map.values().forEach(it -> it.sort(comparator));
            }
            changed = false;
        }
        return map.getOrDefault(elements[0], Collections.emptyList());
    }

    @Override
    public KeyBuffer newKeyBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<T> getList(KeyBuffer key) {
        throw new UnsupportedOperationException();
    }
}
