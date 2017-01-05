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
package com.asakusafw.lang.utils.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A generic I/O resource.
 * @since 0.4.0
 */
public interface Io extends Closeable {

    @Override
    void close() throws IOException;

    /**
     * Initializes {@link Io} objects.
     * @param <T> the object type
     * @since 0.4.0
     */
    class Initializer<T extends Closeable> implements Io {

        private final AtomicReference<T> entity = new AtomicReference<>();

        /**
         * Creates a new instance.
         * @param entity the target object
         */
        public Initializer(T entity) {
            Objects.requireNonNull(entity);
            this.entity.set(entity);
        }

        /**
         * Returns the initializing object.
         * @return the initializing object, of {@code null} if the object is already taken
         */
        public T get() {
            return entity.get();
        }

        /**
         * Takes the initialized object.
         * This initializer will not close the taken object after this operation.
         * @return the initialized object, of {@code null} if the object is already taken
         */
        public T done() {
            return entity.getAndSet(null);
        }

        @Override
        public void close() throws IOException {
            try (Closeable io = entity.getAndSet(null)) {
                Lang.pass(io);
            }
        }
    }

    /**
     * Closes {@link Io} objects.
     * @since 0.4.0
     */
    class Closer implements Io {

        private final LinkedList<Closeable> targets = new LinkedList<>();

        /**
         * Adds an object to be closed.
         * @param <T> the target object
         * @param object the target object
         * @return the target object
         */
        public <T extends Closeable> T add(T object) {
            Objects.requireNonNull(object);
            targets.add(object);
            return object;
        }

        /**
         * Cancels closing added elements.
         */
        public void keep() {
            targets.clear();
        }

        /**
         * Creates a new copy and resets this object.
         * @return the copy
         */
        public Closer move() {
            Closer copy = new Closer();
            copy.targets.addAll(targets);
            targets.clear();
            return copy;
        }

        @Override
        public void close() throws IOException {
            Exception occurred = null;
            while (targets.isEmpty() == false) {
                try {
                    targets.removeLast().close();
                } catch (Exception e) {
                    if (occurred == null) {
                        occurred = e;
                    } else {
                        occurred.addSuppressed(e);
                    }
                }
            }
            if (occurred != null) {
                Lang.rethrow(occurred, IOException.class);
                Lang.rethrow(occurred, RuntimeException.class);
                throw new AssertionError(occurred);
            }
        }
    }
}
