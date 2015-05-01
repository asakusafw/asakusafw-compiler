/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.bridge.broker;

import java.io.Closeable;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

/**
 * Represents a session of resource broker.
 */
public interface ResourceSession extends Closeable {

    /**
     * Returns a registered resource.
     * @param <T> the resource type
     * @param type the resource type
     * @return the corresponded resource, or {@code null} if there is no such a resource in the session
     */
    <T> T find(Class<T> type);

    /**
     * Returns a registered resource.
     * @param <T> the resource type
     * @param type the resource type
     * @return the corresponded resource
     * @throws NoSuchElementException if there is no such a resource in the session
     */
    <T> T get(Class<T> type);

    /**
     * Returns a registered resource, or registers a new resource.
     * @param <T> the resource type
     * @param type the resource type
     * @param supplier the resource supplier, which will be called only if there is no such a resource
     * @return the registered or created resource
     * @throws IllegalStateException if failed to create a new resource via the {@code supplier}
     */
    <T> T get(Class<T> type, Callable<? extends T> supplier);

    /**
     * Registers a resource.
     * @param <T> the resource type
     * @param type the resource type
     * @param resource the target resource
     * @throws IllegalStateException if the target resource already exists in this session
     */
    <T> void put(Class<T> type, T resource);
}
