/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.common;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Utilities for {@link Adaptable}.
 * @since 0.5.2
 */
public final class Adaptables {

    private Adaptables() {
        return;
    }

    /**
     * Returns an adapter from the given stream.
     * @param <T> the adapter type
     * @param type the adapter type
     * @param self the self object
     * @param elements the source stream
     * @return an adapter
     */
    public static <T> Optional<T> find(Class<T> type, Adaptable self, Stream<?> elements) {
        if (type.isInstance(self)) {
            return Optional.of(type.cast(self));
        }
        return find(type, elements);
    }

    /**
     * Returns an adapter from the given stream.
     * @param <T> the adapter type
     * @param type the adapter type
     * @param source the source stream
     * @return an adapter
     */
    public static <T> Optional<T> find(Class<T> type, Stream<?> source) {
        return source
                .flatMap(it -> {
                    if (type.isInstance(it)) {
                        return Stream.of(type.cast(it));
                    }
                    if (it instanceof Adaptable) {
                        return ((Adaptable) it).findAdapter(type)
                                .map(Stream::of)
                                .orElse(Stream.empty());
                    }
                    return Stream.empty();
                })
                .findFirst();
    }
}
