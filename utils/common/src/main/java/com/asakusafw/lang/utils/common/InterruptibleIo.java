/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import java.io.IOException;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A generic interruptible I/O resource.
 * @since 0.4.0
 */
public interface InterruptibleIo extends AutoCloseable {

    @Override
    void close() throws IOException, InterruptedException;

    /**
     * An interface which has initialize operation with {@link IOException} or {@link InterruptedException}.
     */
    @FunctionalInterface
    interface IoInitializable {

        /**
         * Initializes this object.
         * @throws IOException if I/O error was occurred while initializing this object
         * @throws InterruptedException if interrupted while initializing this object
         */
        void initialize() throws IOException, InterruptedException;
    }

    /**
     * {@link java.lang.Runnable} that can throw {@link IOException} or {@link InterruptedException}.
     * @since 0.4.0
     */
    @FunctionalInterface
    interface IoRunnable extends RunnableWithException<Exception> {

        @Override
        void run() throws IOException, InterruptedException;
    }

    /**
     * {@link java.util.concurrent.Callable} that can throw {@link IOException} or {@link InterruptedException}.
     * @param <T> the result type
     * @since 0.4.0
     */
    @FunctionalInterface
    interface IoCallable<T> extends java.util.concurrent.Callable<T> {

        @Override
        T call() throws IOException, InterruptedException;
    }

    /**
     * {@link com.asakusafw.lang.utils.common.Action} that can throw {@link IOException} or
     * {@link InterruptedException}.
     * @param <T> the parameter type
     * @since 0.4.0
     */
    @FunctionalInterface
    interface IoAction<T> extends com.asakusafw.lang.utils.common.Action<T, Exception> {

        @Override
        void perform(T t) throws IOException, InterruptedException;
    }

    /**
     * Initializes {@link InterruptibleIo} objects.
     * @param <T> the object type
     * @since 0.4.0
     */
    class Initializer<T extends InterruptibleIo> implements InterruptibleIo {

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
        public void close() throws IOException, InterruptedException {
            try (InterruptibleIo io = entity.getAndSet(null)) {
                Lang.pass(io);
            }
        }
    }

    /**
     * Closes {@link InterruptibleIo} objects.
     * @since 0.4.0
     */
    class Closer implements InterruptibleIo {

        private final LinkedList<InterruptibleIo> targets = new LinkedList<>();

        /**
         * Adds an object to be closed.
         * @param <T> the target object
         * @param object the target object
         * @return the target object
         */
        public <T extends InterruptibleIo> T add(T object) {
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
        public void close() throws IOException, InterruptedException {
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
                Lang.rethrow(occurred, InterruptedException.class);
                Lang.rethrow(occurred, RuntimeException.class);
                throw new AssertionError(occurred);
            }
        }
    }
}
