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
package com.asakusafw.dag.runtime.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;

/**
 * Basic implementation of {@link DataTable}.
 * @param <T> the data type
 * @since 0.4.0
 */
public class BasicDataTable<T> implements DataTable<T> {

    private final Map<KeyBuffer.View, ? extends List<T>> entity;

    private final Supplier<? extends KeyBuffer> buffers;

    BasicDataTable(Map<KeyBuffer.View, ? extends List<T>> entity, Supplier<? extends KeyBuffer> buffers) {
        this.entity = entity;
        this.buffers = buffers;
    }

    @Override
    public KeyBuffer newKeyBuffer() {
        return buffers.get();
    }

    @Override
    public List<T> getList(KeyBuffer key) {
        List<T> list = entity.get(key.getView());
        if (list == null) {
            return Collections.emptyList();
        }
        return list;
    }

    /**
     * Returns an empty table.
     * @param <T> the data type
     * @return an empty table
     */
    public static <T> DataTable<T> empty() {
        return new BasicDataTable<>(Collections.emptyMap(), () -> VoidKeyBuffer.INSTANCE);
    }

    /**
     * A builder for {@link BasicDataTable}.
     * @param <T> the element type
     */
    public static class Builder<T> implements DataTable.Builder<T> {

        private final Map<KeyBuffer.View, List<T>> entity;

        private final Supplier<? extends KeyBuffer> buffers;

        /**
         * Creates a new instance.
         */
        public Builder() {
            this(new HashMap<>(), HeapKeyBuffer::new);
        }

        /**
         * Creates a new instance.
         * @param entity the table entity
         * @param buffers the key buffer supplier
         */
        public Builder(Map<KeyBuffer.View, List<T>> entity, Supplier<? extends KeyBuffer> buffers) {
            this.entity = entity;
            this.buffers = buffers;
        }

        @Override
        public KeyBuffer newKeyBuffer() {
            return buffers.get();
        }

        @Override
        public DataTable.Builder<T> add(KeyBuffer key, T value) {
            Map<KeyBuffer.View, List<T>> map = entity;
            List<T> list = map.get(key.getView());
            if (list == null) {
                list = new ArrayList<>(1);
                map.put(key.getFrozen(), list);
            }
            list.add(value);
            return this;
        }

        @Override
        public DataTable<T> build() {
            return new BasicDataTable<>(entity, buffers);
        }
    }
}
