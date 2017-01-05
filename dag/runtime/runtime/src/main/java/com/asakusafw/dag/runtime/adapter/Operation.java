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

import java.io.IOException;

/**
 * An abstract super interface of generic operations.
 * @param <T> the operation input type
 * @since 0.4.0
 */
@FunctionalInterface
public interface Operation<T> {

    /**
     * Processes this operation.
     * @param input the input
     * @throws IOException if I/O error was occurred while processing
     * @throws InterruptedException if interrupted while processing
     */
    void process(T input) throws IOException, InterruptedException;
}
