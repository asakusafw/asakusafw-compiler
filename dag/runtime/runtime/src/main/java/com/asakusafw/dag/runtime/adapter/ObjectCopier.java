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
package com.asakusafw.dag.runtime.adapter;

/**
 * Copies objects.
 * @param <T> the data type
 * @since 0.4.0
 */
@FunctionalInterface
public interface ObjectCopier<T> {

    /**
     * Returns a new copy of the source object.
     * @param source the source object
     * @return a copy
     */
    T newCopy(T source);

    /**
     * Returns a new copy of the source object, or the {@code buffer} object which become a copy of the {@code source}.
     * @param source the source object
     * @param buffer the reusable buffer object
     * @return a copy
     */
    default T newCopy(T source, T buffer) {
        return newCopy(source);
    }
}
