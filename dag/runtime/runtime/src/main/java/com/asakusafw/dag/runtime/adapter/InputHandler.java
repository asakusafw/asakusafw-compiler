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
package com.asakusafw.dag.runtime.adapter;

import java.io.IOException;

import com.asakusafw.dag.api.processor.ProcessorContext;

/**
 * An abstract super interface of vertex/task input handlers.
 * @param <T> the input type
 * @param <C> the context type
 * @since 0.4.0
 */
public interface InputHandler<T, C extends ProcessorContext> extends ContextHandler<C> {

    @Override
    InputSession<T> start(C context) throws IOException, InterruptedException;

    /**
     * Represents a session for {@link InputHandler}.
     * @param <T> the input type
     * @since 0.4.0
     */
    public interface InputSession<T> extends Session {

        /**
         * Advances the cursor in the next input, and returns whether or not the next input exists.
         * @throws IOException if I/O error was occurred while reading the next input
         * @throws InterruptedException if interrupted while reading the next input
         * @return {@code true} if the next input exists, otherwise {@code false}
         */
        boolean next() throws IOException, InterruptedException;

        /**
         * Returns the current input.
         * @return the current input
         * @throws IOException if I/O error was occurred while reading the current input
         * @throws InterruptedException if interrupted while reading the current input
         */
        T get() throws IOException, InterruptedException;
    }
}
