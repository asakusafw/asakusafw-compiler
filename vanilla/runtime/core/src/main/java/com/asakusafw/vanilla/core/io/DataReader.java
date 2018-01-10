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
 * An abstract super interface of data reader.
 * @since 0.4.0
 */
public interface DataReader extends InterruptibleIo {

    /**
     * Returns the active buffer of this reader.
     * @return the active buffer, or {@code null} if this does not have an active buffer
     */
    default ByteBuffer getBuffer() {
        return null;
    }

    /**
     * Reads the next integer entry.
     * @return the next integer entry
     * @throws IOException if I/O error was occurred while reading the next entry
     * @throws InterruptedException if interrupted while reading the next entry
     */
    int readInt() throws IOException, InterruptedException;

    /**
     * Reads the next entry into the given buffer.
     * @param destination the destination buffer
     * @throws IOException if I/O error was occurred while reading the next entry
     * @throws InterruptedException if interrupted while reading the next entry
     */
    void readFully(ByteBuffer destination) throws IOException, InterruptedException;

    /**
     * A provider of {@link DataReader}.
     * @since 0.4.0
     */
    interface Provider extends InterruptibleIo {

        /**
         * Opens a new reader.
         * @return the created reader
         * @throws IOException if I/O error was occurred while opening the reader
         * @throws InterruptedException if interrupted while opening the reader
         */
        DataReader open() throws IOException, InterruptedException;
    }

    /**
     * A data input channel.
     * @since 0.4.0
     */
    @FunctionalInterface
    interface Channel {

        /**
         * Takes the next contents from this channel.
         * @return the next reader, or {@code null} if there are no more contents
         * @throws IOException if I/O error was occurred while taking the next contents
         * @throws InterruptedException if interrupted while taking the next contents
         */
        DataReader poll() throws IOException, InterruptedException;
    }
}