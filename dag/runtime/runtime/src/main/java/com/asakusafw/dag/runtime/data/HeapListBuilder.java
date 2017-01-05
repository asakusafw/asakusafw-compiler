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
package com.asakusafw.dag.runtime.data;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;
import java.util.RandomAccess;

import com.asakusafw.dag.api.common.ObjectCursor;

/**
 * A {@link ListBuilder} which provides array backed lists.
 * @param <T> the element type
 * @since 0.4.1
 */
public class HeapListBuilder<T> implements ListBuilder<T> {

    private static final int MIN_ARRAY_SIZE = 256;

    static final Object[] EMPTY = new Object[0];

    private final Entity<T> entity = new Entity<>();

    private final DataAdapter<T> adapter;

    /**
     * Creates a new instance.
     * @param adapter the data adapter
     */
    public HeapListBuilder(DataAdapter<T> adapter) {
        this.adapter = adapter;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> build(ObjectCursor cursor) throws IOException, InterruptedException {
        DataAdapter<T> da = adapter;
        T[] elements = entity.elements;
        int index = 0;
        while (cursor.nextObject()) {
            if (index >= elements.length) {
                elements = Arrays.copyOf(elements, Math.max(elements.length * 2, MIN_ARRAY_SIZE));
            }
            T object = (T) cursor.getObject();
            T destination = elements[index];
            if (destination == null) {
                destination = da.create();
                elements[index] = destination;
            }
            da.copy(object, destination);
            index++;
        }
        entity.elements = elements;
        entity.size = index;
        return entity;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void close() throws IOException, InterruptedException {
        entity.elements = (T[]) EMPTY;
        entity.size = 0;
    }

    @SuppressWarnings("unchecked")
    static final class Entity<T> extends AbstractList<T> implements RandomAccess {

        T[] elements = (T[]) EMPTY;

        int size = 0;

        @Override
        public T get(int index) {
            T[] es = elements;
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException();
            }
            return es[index];
        }

        @Override
        public int size() {
            return size;
        }
    }
}
