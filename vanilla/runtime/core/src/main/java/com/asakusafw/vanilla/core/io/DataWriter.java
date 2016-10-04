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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.asakusafw.lang.utils.common.InterruptibleIo;

/**
 * An abstract super interface of data writer.
 * @since 0.4.0
 */
public interface DataWriter extends InterruptibleIo {

    /**
     * Returns the active buffer of this writer.
     * @return the active buffer, or {@code null} if this does not have an active buffer
     */
    default ByteBuffer getBuffer() {
        return null;
    }

    /**
     * Writes the next integer entry.
     * @param value the integer value
     * @throws IOException if I/O error was occurred while writing the next entry
     * @throws InterruptedException if interrupted while writing the next entry
     */
    void writeInt(int value) throws IOException, InterruptedException;

    /**
     * Writes the next entry from the given buffer.
     * @param source the source buffer, and it will be fully consumed
     * @throws IOException if I/O error was occurred while writing the next entry
     * @throws InterruptedException if interrupted while writing the next entry
     */
    void writeFully(ByteBuffer source) throws IOException, InterruptedException;

    /**
     * A data output channel.
     * @since 0.4.0
     */
    interface Channel {

        /**
         * Acquires a new writer.
         * @param size the estimated data size
         * @return the acquired writer
         * @throws IOException if I/O error was occurred while acquiring the writer
         * @throws InterruptedException if interrupted while acquiring the writer
         */
        DataWriter acquire(int size) throws IOException, InterruptedException;

        /**
         * Commits the written data.
         * @param written the writer
         * @throws IOException if I/O error was occurred while committing the writer
         * @throws InterruptedException if interrupted while committing the writer
         */
        void commit(DataWriter written) throws IOException, InterruptedException;
    }
}
