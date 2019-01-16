/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
 * A sorted key-value pair records reader.
 * @since 0.4.0
 */
public interface KeyValueCursor extends InterruptibleIo {

    /**
     * Advances the cursor and returns whether or not the next record exists.
     * @return {@code true} if the next object exists, otherwise {@code false}
     * @throws IOException if I/O error occurred while reading the next record
     * @throws InterruptedException if interrupted while reading the next record
     */
    boolean next() throws IOException, InterruptedException;

    /**
     * Returns a buffer slice of the next key.
     * @return the next key
     * @throws IOException if I/O error was occurred while opening the key
     * @throws InterruptedException if interrupted while opening the key
     */
    ByteBuffer getKey() throws IOException, InterruptedException;

    /**
     * Returns a buffer slice of the next value.
     * @return the next value
     * @throws IOException if I/O error was occurred while opening the value
     * @throws InterruptedException if interrupted while opening the value
     */
    ByteBuffer getValue() throws IOException, InterruptedException;
}