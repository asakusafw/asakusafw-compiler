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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.asakusafw.lang.utils.common.InterruptibleIo;

/**
 * Accepts key value pairs.
 * @since 0.4.0
 */
public interface KeyValueSink extends InterruptibleIo {

    /**
     * Accepts key-value pair.
     * @param key the key buffer
     * @param value the value buffer
     * @throws IOException if I/O error was occurred while processing the pair
     * @throws InterruptedException if interrupted while processing the pair
     */
    void accept(ByteBuffer key, ByteBuffer value) throws IOException, InterruptedException;

    /**
     * Accepts a value for the last accepted key.
     * @param value the value buffer
     * @return {@code true} if this accepted the value
     * @throws IOException if I/O error was occurred while processing the pair
     * @throws InterruptedException if interrupted while processing the pair
     */
    default boolean accept(ByteBuffer value) throws IOException, InterruptedException {
        return false;
    }

    /**
     * A stream of {@link KeyValueSink}.
     * @since 0.4.0
     */
    @FunctionalInterface
    interface Stream {

        /**
         * Offers the next sink.
         * @param recordCount the number of records
         * @param keySize the total key size in bytes
         * @param valueSize the total value size in bytes
         * @return the next sink
         * @throws IOException if I/O error was occurred while creating a sink
         * @throws InterruptedException if interrupted while creating a sink
         */
        KeyValueSink offer(int recordCount, int keySize, int valueSize) throws IOException, InterruptedException;
    }
}
