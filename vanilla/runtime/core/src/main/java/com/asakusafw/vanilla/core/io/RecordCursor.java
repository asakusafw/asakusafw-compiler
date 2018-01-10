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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.asakusafw.lang.utils.common.InterruptibleIo;

/**
 * A record reader.
 * @since 0.4.0
 */
public interface RecordCursor extends InterruptibleIo {

    /**
     * Advances the cursor and returns whether or not the next record exists.
     * @return {@code true} if the next object exists, otherwise {@code false}
     * @throws IOException if I/O error occurred while reading the next record
     * @throws InterruptedException if interrupted while reading the next record
     */
    boolean next() throws IOException, InterruptedException;

    /**
     * Returns a buffer slice of the next record.
     * @return the next record
     * @throws IOException if I/O error was occurred while opening the record
     * @throws InterruptedException if interrupted while opening the record
     */
    ByteBuffer get() throws IOException, InterruptedException;

    /**
     * A stream of {@link RecordCursor}.
     * @since 0.4.0
     */
    @FunctionalInterface
    interface Stream {

        /**
         * Takes the next element.
         * @return the next element, or {@code null} if the next element is not available
         * @throws IOException if I/O error was occurred while taking the next element
         * @throws InterruptedException if interrupted while taking the next element
         */
        RecordCursor poll() throws IOException, InterruptedException;
    }
}