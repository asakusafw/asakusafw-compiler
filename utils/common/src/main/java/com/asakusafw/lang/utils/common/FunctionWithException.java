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

/**
 * Represents a function which may raise exceptions.
 * @param <T> the argument type
 * @param <R> the result type
 * @param <E> the throwable exception type
 * @since 0.4.0
 */
@FunctionalInterface
public interface FunctionWithException<T, R, E extends Exception> {

    /**
     * Applies this function to the given argument.
     * @param argument the argument
     * @return the result
     * @throws E if an exception was occurred while applying the function
     */
    R apply(T argument) throws E;
}
