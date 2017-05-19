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
package com.asakusafw.dag.runtime.table;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.dag.runtime.adapter.KeyBuffer;

/**
 * Basic implementation of {@link DataTable}.
 * @param <T> the data type
 * @since 0.4.0
 * @version 0.4.1
 */
public class BasicDataTable<T> implements DataTable<T> {

    private final Map<KeyBuffer.View, ? extends List<T>> entity;

    private final Supplier<? extends KeyBuffer> buffers;

    private final ThreadLocal<KeyBuffer> bufferCache;

    private final int keyElementCount;

    private final Class<?>[] keyElementTypes;

    private boolean keyElementTypeValidation;

    BasicDataTable(Map<KeyBuffer.View, ? extends List<T>> entity, Supplier<? extends KeyBuffer> buffers) {
        this(entity, buffers, KeyValidator.NULL);
    }

    BasicDataTable(
            Map<KeyBuffer.View, ? extends List<T>> entity,
            Supplier<? extends KeyBuffer> buffers,
            KeyValidator validator) {
        this.entity = entity;
        this.buffers = buffers;
        this.bufferCache = ThreadLocal.withInitial(buffers);
        this.keyElementCount = validator.level == ValidationLevel.NOTHING ? -1 : validator.types.length;
        this.keyElementTypes = validator.types;
        this.keyElementTypeValidation = validator.level == ValidationLevel.TYPE;
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

    @Override
    public Iterator<T> iterator() {
        Iterator<? extends List<T>> partitions = entity.values().iterator();
        return new Iterator<T>() {
            private Iterator<T> nextPartition;
            @Override
            public boolean hasNext() {
                while (true) {
                    if (nextPartition == null) {
                        if (partitions.hasNext()) {
                            nextPartition = partitions.next().iterator();
                        } else {
                            return false;
                        }
                    }
                    if (nextPartition.hasNext()) {
                        return true;
                    } else {
                        nextPartition = null;
                    }
                }
            }

            @Override
            public T next() {
                if (nextPartition == null) {
                    throw new NoSuchElementException();
                }
                return nextPartition.next();
            }
        };
    }

    @Override
    public List<T> find() {
        // (>= 0 && != 0) -> (> 0)
        if (keyElementCount > 0) {
            throw incompatible();
        }
        KeyBuffer buffer = bufferCache.get().clear();
        return getList(buffer);
    }

    @Override
    public List<T> find(Object key) {
        if (keyElementCount >= 0) {
            if (keyElementCount != 1) {
                throw incompatible(key);
            }
            if (keyElementTypes != null) {
                checkType(0, key);
            }
        }
        KeyBuffer buffer = bufferCache.get().clear();
        buffer.append(key);
        return getList(buffer);
    }

    @Override
    public List<T> find(Object a, Object b) {
        int count = keyElementCount;
        if (count >= 0) {
            if (count != 2) {
                throw incompatible(a, b);
            }
            checkType(0, a);
            checkType(1, b);
        }
        KeyBuffer buffer = bufferCache.get().clear();
        buffer.append(a);
        buffer.append(b);
        return getList(buffer);
    }

    @Override
    public List<T> find(Object a, Object b, Object c) {
        int count = keyElementCount;
        if (count >= 0) {
            if (count != 3) {
                throw incompatible(a, b, c);
            }
            checkType(0, a);
            checkType(1, b);
            checkType(2, c);
        }
        KeyBuffer buffer = bufferCache.get().clear();
        buffer.append(a);
        buffer.append(b);
        buffer.append(c);
        return getList(buffer);
    }

    @Override
    public List<T> find(Object a, Object b, Object c, Object d) {
        int count = keyElementCount;
        if (count >= 0) {
            if (count != 4) {
                throw incompatible(a, b, c, d);
            }
            checkType(0, a);
            checkType(1, b);
            checkType(2, c);
            checkType(3, d);
        }
        KeyBuffer buffer = bufferCache.get().clear();
        buffer.append(a);
        buffer.append(b);
        buffer.append(c);
        buffer.append(d);
        return getList(buffer);
    }

    @Override
    public List<T> find(Object... elements) {
        int count = keyElementCount;
        if (count >= 0) {
            if (count != elements.length) {
                throw incompatible(elements);
            }
            for (int i = 0; i < count; i++) {
                checkType(i, elements[i]);
            }
        }
        KeyBuffer buffer = bufferCache.get().clear();
        for (Object element : elements) {
            buffer.append(element);
        }
        return getList(buffer);
    }

    private void checkType(int index, Object element) {
        if (keyElementTypeValidation == false) {
            return;
        }
        if (element == null) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "key element must be never null (at {0})",
                    index));
        }
        assert keyElementTypes != null;
        assert index < keyElementTypes.length;
        Class<? extends Object> actual = element.getClass();
        if (actual.equals(keyElementTypes[index]) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "key element at {0} has an inconsistent type: required={1}, actual={2}",
                    index,
                    keyElementTypes[index].getName(),
                    actual.getName()));
        }
    }

    private IllegalArgumentException incompatible(Object... elements) {
        assert keyElementTypes != null;
        String expected = Arrays.stream(keyElementTypes)
                .map(Class::getSimpleName)
                .collect(Collectors.joining(", ")); //$NON-NLS-1$
        String actual = Arrays.stream(elements)
                .map(e -> e == null ? null : e.getClass().getSimpleName())
                .collect(Collectors.joining(", ")); //$NON-NLS-1$
        return new IllegalArgumentException(MessageFormat.format(
                "defined key is [{0}], but requested key is [{1}]",
                expected,
                actual));
    }

    /**
     * A builder for {@link BasicDataTable}.
     * @param <T> the element type
     * @since 0.4.0
     * @version 0.4.1
     */
    public static class Builder<T> implements DataTable.Builder<T> {

        private final Map<KeyBuffer.View, List<T>> entity;

        private final Supplier<? extends KeyBuffer> buffers;

        private final KeyValidator validator;

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
            this(entity, buffers, KeyValidator.NULL);
        }

        /**
         * Creates a new instance.
         * @param entity the table entity
         * @param buffers the key buffer supplier
         * @param validator the key validator
         * @since 0.4.1
         */
        public Builder(
                Map<KeyBuffer.View, List<T>> entity,
                Supplier<? extends KeyBuffer> buffers,
                KeyValidator validator) {
            this.entity = entity;
            this.buffers = buffers;
            this.validator = validator;
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
        public DataTable<T> build(Comparator<? super T> comparator) {
            if (comparator != null) {
                for (List<T> entry : entity.values()) {
                    entry.sort(comparator);
                }
            }
            return new BasicDataTable<>(entity, buffers, validator);
        }
    }

    /**
     * A validator for {@link BasicDataTable}.
     * @since 0.4.1
     */
    public static class KeyValidator {

        /**
         * Never validates any keys.
         */
        public static final KeyValidator NULL = new KeyValidator(ValidationLevel.NOTHING);

        final ValidationLevel level;

        final Class<?>[] types;

        /**
         * Creates a new instance.
         * @param level the validation level
         * @param types the element types
         */
        public KeyValidator(ValidationLevel level, Class<?>... types) {
            this.level = level;
            this.types = types.clone();
        }
    }

    /**
     * Represents validation depth of {@link KeyValidator}.
     * @since 0.4.1
     */
    public enum ValidationLevel {

        /**
         * Validates nothing.
         */
        NOTHING,

        /**
         * Validates only the number of key elements.
         */
        COUNT,

        /**
         * Validates each type of key element.
         */
        TYPE,
    }
}
