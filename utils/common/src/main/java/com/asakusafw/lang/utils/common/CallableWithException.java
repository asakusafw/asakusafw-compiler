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

import java.util.concurrent.Callable;

/**
 * Represents a program fragment.
 * @param <V> the result type
 * @param <E> throwable exception type
 * @since 0.4.0
 */
@FunctionalInterface
public interface CallableWithException<V, E extends Exception> extends Callable<V> {

    /**
     * Performs this callable.
     * @return the computed result
     * @throws E if an exception was occurred
     */
    @Override
    V call() throws E;
}
