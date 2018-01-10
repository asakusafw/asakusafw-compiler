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
 * Represents a buffer pool.
 * Note that, the pool only manages its pool size, and does not implement {@link AutoCloseable}.
 * Framework developers must dispose each buffer out of the pool.
 * @since 0.4.0
 */
public interface BufferPool {

    /**
     * Returns the estimated total buffer size in this pool.
     * @return the pool size in bytes
     */
    long getSize();

    /**
     * Reserves a new buffer fragment.
     * @param size the estimated buffer size in bytes
     * @return the reservation ticket
     * @throws IOException if I/O error was occurred while reserving a new buffer
     * @throws InterruptedException if interrupted while reserving a new buffer
     */
    Ticket reserve(long size) throws IOException, InterruptedException;

    /**
     * Registers the given buffer into this pool.
     * Clients must not change contents of the buffer after this operation.
     * @param ticket the ticket of the given buffer
     * @param buffer the buffer
     * @return the buffer entry
     * @throws IOException if I/O error was occurred while registering the buffer
     * @throws InterruptedException if interrupted while registering the buffer
     * @see Ticket#move()
     */
    DataReader.Provider register(Ticket ticket, ByteBuffer buffer) throws IOException, InterruptedException;

    /**
     * Registers the given buffer into this pool.
     * Clients must not change contents of the buffer after this operation.
     * @param ticket the ticket of the given buffer
     * @param buffer the buffer
     * @param priority the priority of given buffer (higher priority may be long lived)
     * @return the buffer entry
     * @throws IOException if I/O error was occurred while registering the buffer
     * @throws InterruptedException if interrupted while registering the buffer
     * @see Ticket#move()
     */
    default DataReader.Provider register(
            Ticket ticket, ByteBuffer buffer, int priority) throws IOException, InterruptedException {
        return register(ticket, buffer);
    }

    /**
     * Represents a ticket of buffer area reservation.
     * @since 0.4.0
     */
    interface Ticket extends InterruptibleIo {

        /**
         * Returns the reserved buffer size by this ticket.
         * @return the reserved buffer size in bytes, or {@code 0} if the ticket has been already closed
         */
        long getSize();

        /**
         * Creates a copy of this and releases this.
         * @return the created copy
         */
        Ticket move();

        /**
         * Releases this ticket.
         * @throws IOException if I/O error was occurred while releasing the ticket
         * @throws InterruptedException if interrupted while releasing the ticket
         */
        @Override
        void close() throws IOException, InterruptedException;
    }
}
