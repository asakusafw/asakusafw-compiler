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
package com.asakusafw.lang.compiler.common.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Utilities of {@link Enum} classes.
 */
public final class EnumUtil {

    private EnumUtil() {
        return;
    }

    /**
     * Returns unmodifiable set.
     * @param <T> the enum type
     * @param elements the elements
     * @return the unmodifiable set
     */
    public static <T extends Enum<T>> Set<T> freeze(Collection<T> elements) {
        if (elements.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(EnumSet.copyOf(elements));
    }

    /**
     * Returns unmodifiable set.
     * @param <T> the enum type
     * @param elements the elements
     * @return the unmodifiable set
     */
    @SafeVarargs
    public static <T extends Enum<T>> Set<T> freeze(T... elements) {
        return freeze(Arrays.asList(elements));
    }

    /**
     * Returns unmodifiable map.
     * @param <K> the enum type
     * @param <V> the map value type
     * @param elements the elements
     * @return the unmodifiable map
     */
    public static <K extends Enum<K>, V> Map<K, V> freeze(Map<K, ? extends V> elements) {
        if (elements.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new EnumMap<>(elements));
    }
}
