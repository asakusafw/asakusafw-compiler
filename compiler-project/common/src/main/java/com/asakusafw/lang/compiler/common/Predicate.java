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
package com.asakusafw.lang.compiler.common;

/**
 * Represents a predicate.
 * @param <T> member type
 */
public interface Predicate<T> {

    /**
     * Returns whether the argument satisfies this predicate or not.
     * @param argument the target argument
     * @return {@code true} if the argument satisfies this, otherwise {@code false}
     */
    boolean apply(T argument);
}
