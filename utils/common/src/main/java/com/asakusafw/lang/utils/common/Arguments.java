/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utilities for method arguments.
 * @since 0.4.0
 */
public final class Arguments {

    private Arguments() {
        return;
    }

    /**
     * Maps a nullable value.
     * @param <T> the value type
     * @param <R> the mapped type
     * @param valueOrNull the value (nullable)
     * @param function the function for non-null values
     * @return the mapped value, or {@code null} if the value is {@code null}
     */
    public static <T, R> R map(T valueOrNull, Function<? super T, ? extends R> function) {
        requireNonNull(function);
        return Optional.ofNullable(valueOrNull)
                .map(function)
                .orElse(null);
    }

    /**
     * Maps a nullable value.
     * @param <T> the value type
     * @param <R> the mapped type
     * @param valueOrNull the value (nullable)
     * @param function the function for non-null values
     * @param defaultSupplier the supplier for null values
     * @return the mapped value, or supplied value (from {@code defaultSupplier}) if the value is {@code null}
     */
    public static <T, R> R map(
            T valueOrNull,
            Function<? super T, ? extends R> function,
            Supplier<? extends R> defaultSupplier) {
        requireNonNull(function);
        requireNonNull(defaultSupplier);
        return Optional.ofNullable(valueOrNull)
                .<R>map(function)
                .orElseGet(defaultSupplier);
    }

    /**
     * Raises an exception if the target value is {@code null}.
     * @param <T> the value type
     * @param value the value
     * @return the value
     * @throws IllegalArgumentException if the value is {@code null}
     */
    public static <T> T requireNonNull(T value) {
        require(value != null);
        return value;
    }

    /**
     * Raises an exception if the target value is {@code null}.
     * @param <T> the value type
     * @param value the value
     * @param message the exception message
     * @return the value
     * @throws IllegalArgumentException if the value is {@code null}
     */
    public static <T> T requireNonNull(T value, String message) {
        require(value != null, message);
        return value;
    }

    /**
     * Raises an exception if the target value is {@code null}.
     * @param <T> the value type
     * @param value the value
     * @param messageSupplier the exception message supplier
     * @return the value
     * @throws IllegalArgumentException if the value is {@code null}
     */
    public static <T> T requireNonNull(T value, Supplier<?> messageSupplier) {
        require(value != null, messageSupplier);
        return value;
    }

    /**
     * Raises an exception if the condition is {@code false}.
     * @param condition the condition value
     * @throws IllegalArgumentException if the condition is {@code false}
     */
    public static void require(boolean condition) {
        if (condition == false) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Raises an exception if the condition is {@code false}.
     * @param condition the condition value
     * @param message the exception message
     * @throws IllegalArgumentException if the condition is {@code false}
     */
    public static void require(boolean condition, String message) {
        if (condition == false) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Raises an exception if the condition is {@code false}.
     * @param condition the condition value
     * @param messageSupplier the exception message supplier
     * @throws IllegalArgumentException if the condition is {@code false}
     */
    public static void require(boolean condition, Supplier<?> messageSupplier) {
        if (condition == false) {
            throw new IllegalArgumentException(message(messageSupplier));
        }
    }

    /**
     * Raises an exception only if the process throws an exception.
     * @param <T> the callable result type
     * @param process the target callable
     * @return the callable result
     * @throws IllegalArgumentException if the callable throws an exception
     */
    public static <T> T safe(Callable<T> process) {
        try {
            return process.call();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Raises an exception only if the process throws an exception.
     * @param <T> the callable result type
     * @param process the target callable
     * @param message the exception message
     * @return the callable result
     * @throws IllegalArgumentException if the callable throws an exception
     */
    public static <T> T safe(Callable<T> process, String message) {
        try {
            return process.call();
        } catch (Exception e) {
            throw new IllegalArgumentException(message, e);
        }
    }

    /**
     * Raises an exception only if the process throws an exception.
     * @param <T> the callable result type
     * @param process the target callable
     * @param messageSupplier the exception message supplier
     * @return the callable result
     * @throws IllegalArgumentException if the callable throws an exception
     */
    public static <T> T safe(Callable<T> process, Supplier<?> messageSupplier) {
        try {
            return process.call();
        } catch (Exception e) {
            throw new IllegalArgumentException(message(messageSupplier), e);
        }
    }

    /**
     * Creates a copy of the list.
     * @param <T> the element type
     * @param list the target list
     * @return the created copy
     */
    public static <T> List<T> copy(List<? extends T> list) {
        Arguments.requireNonNull(list);
        return new ArrayList<>(list);
    }

    /**
     * Creates a frozen copy of the list.
     * @param <T> the element type
     * @param list the target list
     * @return the created copy
     */
    public static <T> List<T> freeze(List<? extends T> list) {
        Arguments.requireNonNull(list);
        if (list.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(copy(list));
    }

    /**
     * Creates a copy of the set.
     * @param <T> the element type
     * @param set the target set
     * @return the created copy
     */
    public static <T> Set<T> copy(Set<? extends T> set) {
        Arguments.requireNonNull(set);
        return new LinkedHashSet<>(set);
    }

    /**
     * Creates a frozen copy of the set.
     * @param <T> the element type
     * @param set the target set
     * @return the created copy
     */
    public static <T> Set<T> freeze(Set<? extends T> set) {
        Arguments.requireNonNull(set);
        if (set.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(copy(set));
    }

    /**
     * Creates a copy of the set.
     * @param <T> the element type
     * @param set the target set
     * @return the created copy
     */
    public static <T> SortedSet<T> copy(SortedSet<T> set) {
        Arguments.requireNonNull(set);
        return new TreeSet<>(set);
    }

    /**
     * Creates a frozen copy of the set.
     * @param <T> the element type
     * @param set the target set
     * @return the created copy
     */
    public static <T> SortedSet<T> freeze(SortedSet<T> set) {
        Arguments.requireNonNull(set);
        if (set.isEmpty()) {
            return Collections.emptySortedSet();
        }
        return Collections.unmodifiableSortedSet(copy(set));
    }

    /**
     * Creates a copy of the map.
     * @param <K> the key type
     * @param <V> the value type
     * @param map the target map
     * @return the created copy
     */
    public static <K, V> Map<K, V> copy(Map<? extends K, ? extends V> map) {
        Arguments.requireNonNull(map);
        return new LinkedHashMap<>(map);
    }

    /**
     * Creates a frozen copy of the map.
     * @param <K> the key type
     * @param <V> the value type
     * @param map the target map
     * @return the created copy
     */
    public static <K, V> Map<K, V> freeze(Map<? extends K, ? extends V> map) {
        Arguments.requireNonNull(map);
        if (map.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(copy(map));
    }

    /**
     * Creates a copy of the map.
     * @param <K> the key type
     * @param <V> the value type
     * @param map the target map
     * @return the created copy
     */
    public static <K, V> SortedMap<K, V> copy(SortedMap<K, ? extends V> map) {
        Arguments.requireNonNull(map);
        return new TreeMap<>(map);
    }

    /**
     * Creates a frozen copy of the map.
     * @param <K> the key type
     * @param <V> the value type
     * @param map the target map
     * @return the created copy
     */
    public static <K, V> SortedMap<K, V> freeze(SortedMap<K, ? extends V> map) {
        Arguments.requireNonNull(map);
        if (map.isEmpty()) {
            return Collections.emptySortedMap();
        }
        return Collections.unmodifiableSortedMap(copy(map));
    }

    /**
     * Creates a copy of the list.
     * @param <T> the element type
     * @param collection the target collection
     * @return the created copy
     */
    public static <T> List<T> copyToList(Collection<? extends T> collection) {
        Arguments.requireNonNull(collection);
        return new ArrayList<>(collection);
    }

    /**
     * Creates a frozen copy of the list.
     * @param <T> the element type
     * @param collection the target collection
     * @return the created copy
     */
    public static <T> List<T> freezeToList(Collection<? extends T> collection) {
        Arguments.requireNonNull(collection);
        if (collection.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(copyToList(collection));
    }

    /**
     * Creates a copy of the set.
     * @param <T> the element type
     * @param collection the target collection
     * @return the created copy
     */
    public static <T> Set<T> copyToSet(Collection<? extends T> collection) {
        Arguments.requireNonNull(collection);
        return new LinkedHashSet<>(collection);
    }

    /**
     * Creates a frozen copy of the set.
     * @param <T> the element type
     * @param collection the target collection
     * @return the created copy
     */
    public static <T> Set<T> freezeToSet(Collection<? extends T> collection) {
        Arguments.requireNonNull(collection);
        if (collection.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(copyToSet(collection));
    }

    /**
     * Creates a copy of the list.
     * @param <T> the element type
     * @param elements the target list
     * @return the created copy
     */
    @SafeVarargs
    public static <T> List<T> copyToList(T... elements) {
        Arguments.requireNonNull(elements);
        ArrayList<T> results = new ArrayList<>();
        Collections.addAll(results, elements);
        return results;
    }

    /**
     * Creates a frozen copy of the list.
     * @param <T> the element type
     * @param elements the target list
     * @return the created copy
     */
    @SafeVarargs
    public static <T> List<T> freezeToList(T... elements) {
        Arguments.requireNonNull(elements);
        if (elements.length == 0) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(copyToList(elements));
    }

    /**
     * Creates a copy of the set.
     * @param <T> the element type
     * @param elements the target set
     * @return the created copy
     */
    @SafeVarargs
    public static <T> Set<T> copyToSet(T... elements) {
        Arguments.requireNonNull(elements);
        LinkedHashSet<T> results = new LinkedHashSet<>();
        Collections.addAll(results, elements);
        return results;
    }

    /**
     * Creates a frozen copy of the set.
     * @param <T> the element type
     * @param elements the target set
     * @return the created copy
     */
    @SafeVarargs
    public static <T> Set<T> freezeToSet(T... elements) {
        Arguments.requireNonNull(elements);
        if (elements.length == 0) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(copyToSet(elements));
    }

    private static String message(Supplier<?> supplier) {
        Object value = supplier.get();
        return value == null ? null : value.toString();
    }
}
