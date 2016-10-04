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
package com.asakusafw.dag.api.common;

import java.io.DataInput;
import java.io.IOException;

/**
 * Deserializes key-value pairs.
 * @since 0.4.0
 */
public interface KeyValueDeserializer {

    /**
     * Returns the next key object from the key {@link DataInput}.
     * @param keyInput the key input
     * @return the next key object
     * @throws IOException if I/O error was occurred while reading the next object
     * @throws InterruptedException if interrupted while reading the next object
     */
    default Object deserializeKey(DataInput keyInput) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a next object from the key-value pair of {@link DataInput}.
     * @param keyInput the key input
     * @param valueInput the value input
     * @return the next object
     * @throws IOException if I/O error was occurred while reading the next object
     * @throws InterruptedException if interrupted while reading the next object
     */
    Object deserializePair(DataInput keyInput, DataInput valueInput) throws IOException, InterruptedException;
}
