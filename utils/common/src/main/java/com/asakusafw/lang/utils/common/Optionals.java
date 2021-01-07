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

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Utilities for {@link Optional}.
 * @since 0.4.0
 */
public final class Optionals {

    private Optionals() {
        return;
    }

    /**
     * Returns an empty {@link Optional}.
     * @param <T> the value type
     * @return the optional object
     */
    public static <T> Optional<T> empty() {
        return Optional.empty();
    }

    /**
     * Creates a new {@link Optional} object from a nullable value.
     * @param <T> the value type
     * @param valueOrNull the nullable value
     * @return the optional object
     */
    public static <T> Optional<T> optional(T valueOrNull) {
        return Optional.ofNullable(valueOrNull);
    }

    /**
     * Creates a new {@link Optional} object from a nullable value.
     * @param <T> the value type
     * @param valueOrNull the nullable value
     * @return the optional object
     */
    public static <T> Optional<T> of(T valueOrNull) {
        return optional(valueOrNull);
    }

    /**
     * Converts an optional object into a stream.
     * @param <T> the value type
     * @param valueOrNull the nullable value
     * @return the corresponded stream
     */
    public static <T> Stream<T> stream(T valueOrNull) {
        if (valueOrNull == null) {
            return Stream.empty();
        } else {
            return Stream.of(valueOrNull);
        }
    }

    /**
     * Returns a optional value from the map.
     * @param <K> the key type
     * @param <V> the value type
     * @param map the target map
     * @param key the key for the optional value
     * @return the optional value
     */
    public static <K, V> Optional<V> get(Map<K, V> map, K key) {
        Arguments.requireNonNull(map);
        return optional(map.get(key));
    }

    /**
     * Puts a value into the map only if the value is not {@code null}.
     * @param <K> the key type
     * @param <V> the value type
     * @param map the target map
     * @param key the key for the optional value
     * @param valueOrNull the nullable value
     * @return the target map
     */
    public static <K, V> Map<K, V> put(Map<K, V> map, K key, V valueOrNull) {
        Arguments.requireNonNull(map);
        if (valueOrNull != null) {
            map.put(key, valueOrNull);
        }
        return map;
    }

    /**
     * Removes a optional value from the map.
     * @param <K> the key type
     * @param <V> the value type
     * @param map the target map
     * @param key the key for the optional value
     * @return the optional value
     */
    public static <K, V> Optional<V> remove(Map<K, V> map, K key) {
        Arguments.requireNonNull(map);
        return optional(map.remove(key));
    }
}
