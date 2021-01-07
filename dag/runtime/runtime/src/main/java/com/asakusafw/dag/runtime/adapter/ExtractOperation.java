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
package com.asakusafw.dag.runtime.adapter;

import java.io.IOException;

/**
 * An abstract super interface of extract like operations.
 * @since 0.4.0
 */
public interface ExtractOperation extends Operation<ExtractOperation.Input> {

    /**
     * An abstract interface of input for {@link ExtractOperation}.
     * @since 0.4.0
     */
    public interface Input {

        /**
         * Returns an object.
         * @param <T> the object type
         * @return the object
         * @throws IOException if I/O error was occurred while reading the input
         * @throws InterruptedException if interrupted while reading the input
         */
        <T> T getObject() throws IOException, InterruptedException;
    }
}
