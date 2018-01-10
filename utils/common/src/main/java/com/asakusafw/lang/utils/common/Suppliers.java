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

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Utilities for {@link Supplier}.
 * @since 0.4.0
 */
public final class Suppliers {

    private Suppliers() {
        return;
    }

    /**
     * Returns an empty supplier.
     * @param <T> the value type
     * @return the created supplier
     */
    public static <T> Supplier<T> supplier() {
        return () -> null;
    }

    /**
     * Returns a supplier which supplies the specified value only once.
     * @param <T> the value type
     * @param value the target value
     * @return the created supplier
     */
    public static <T> Supplier<T> supplier(T value) {
        Arguments.requireNonNull(value);
        AtomicReference<T> ref = new AtomicReference<>(value);
        return () -> ref.getAndSet(null);
    }

    /**
     * Returns a supplier which supplies the specified values.
     * @param <T> the value type
     * @param values the values
     * @return the created supplier
     */
    @SafeVarargs
    public static <T> Supplier<T> supplier(T... values) {
        Arguments.requireNonNull(values);
        LinkedList<T> list = new LinkedList<>();
        Collections.addAll(list, values);
        return list::pollFirst;
    }

    /**
     * Returns a supplier which supplies the specified values.
     * @param <T> the value type
     * @param values the values
     * @return the created supplier
     */
    @SafeVarargs
    public static <T> Supplier<T> of(T... values) {
        return supplier(values);
    }

    /**
     * Returns a supplier which supplies the specified values.
     * @param <T> the value type
     * @param values the values
     * @return the created supplier
     */
    public static <T> Supplier<T> fromIterable(Iterable<? extends T> values) {
        Arguments.requireNonNull(values);
        LinkedList<T> list = new LinkedList<>();
        for (T value : values) {
            list.add(value);
        }
        return list::pollFirst;
    }

    /**
     * Returns a supplier which supplies results of the {@link Callable}.
     * The supplier may raises {@link IllegalStateException} if the {@link Callable#call()} raises an exception.
     * @param <T> the value type
     * @param callable the target callable
     * @return the created supplier
     */
    public static <T> Supplier<T> fromCallable(Callable<T> callable) {
        return () -> {
            try {
                return callable.call();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        };
    }
}
