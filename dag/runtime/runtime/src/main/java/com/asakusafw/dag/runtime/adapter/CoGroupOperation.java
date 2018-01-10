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
package com.asakusafw.dag.runtime.adapter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.asakusafw.dag.api.common.ObjectCursor;

/**
 * An abstract super interface of co-group like operations.
 * @since 0.4.0
 */
public interface CoGroupOperation extends Operation<CoGroupOperation.Input> {

    /**
     * An abstract interface of input for {@link CoGroupOperation}.
     * @since 0.4.0
     */
    public interface Input {

        /**
         * Returns a co-group element.
         * @param <T> the element type
         * @param index the group index (0-origin)
         * @return the group elements
         * @throws IOException if I/O error was occurred while reading the input
         * @throws InterruptedException if interrupted while reading the input
         */
        <T> Cursor<T> getCursor(int index) throws IOException, InterruptedException;

        /**
         * Returns a co-group element.
         * @param <T> the element type
         * @param index the group index (0-origin)
         * @return the group elements
         * @throws IOException if I/O error was occurred while reading the input
         * @throws InterruptedException if interrupted while reading the input
         */
        <T> List<T> getList(int index) throws IOException, InterruptedException;
    }

    /**
     * Provides the element in each group.
     * @param <T> the element type
     * @since 0.4.0
     * @version 0.4.1
     */
    public interface Cursor<T> extends ObjectCursor, Iterator<T> {

        @Override
        T getObject() throws IOException, InterruptedException;

        @Override
        default boolean hasNext() {
            try {
                return nextObject();
            } catch (IOException | InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        default T next() {
            try {
                return getObject();
            } catch (IOException | InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
