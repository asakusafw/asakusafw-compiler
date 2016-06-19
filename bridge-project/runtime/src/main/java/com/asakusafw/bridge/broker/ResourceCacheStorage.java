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
package com.asakusafw.bridge.broker;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A thread-local resource cache storage for {@link ResourceBroker}.
 * @param <V> the value type
 * @since 0.3.1
 */
public class ResourceCacheStorage<V> {

    private final ThreadLocal<Entry<V>> storage = new ThreadLocal<Entry<V>>() {
        @Override
        protected ResourceCacheStorage.Entry<V> initialValue() {
            return new Entry<>(null);
        }
    };

    // TODO public V get(Supplier<V> defaultValue) = Optional.ofNullable(find()).orElseGet(() -> put(supplier.get))

    /**
     * Returns the cached value in this storage.
     * @return the cached value, or {@code null} if there are no corresponded value in this storage for
     * the current thread and resource session
     */
    public V find() {
        return storage.get().get();
    }

    /**
     * Puts a value to this storage.
     * The value will be accessible only from the current session and thread.
     * @param value the value
     * @return the argument
     */
    public V put(V value) {
        Entry<V> entry = new Entry<>(value);
        storage.set(entry);
        ResourceBroker.schedule(entry);
        return value;
    }

    private static final class Entry<V> implements AutoCloseable {

        private final AtomicReference<V> reference;

        Entry(V value) {
            this.reference = new AtomicReference<>(value);
        }

        V get() {
            return reference.get();
        }

        @Override
        public void close() throws Exception {
            reference.set(null);
        }
    }
}
