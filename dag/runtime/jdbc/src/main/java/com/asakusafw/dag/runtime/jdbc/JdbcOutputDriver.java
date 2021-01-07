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
package com.asakusafw.dag.runtime.jdbc;

import java.io.IOException;
import java.sql.Connection;

import com.asakusafw.dag.api.common.ObjectSink;
import com.asakusafw.lang.utils.common.InterruptibleIo;

/**
 * Processes output into JDBC.
 * @since 0.4.0
 */
@FunctionalInterface
public interface JdbcOutputDriver {

    /**
     * Creates a new writer which accepts each output object.
     * The sink never commits the current transaction, so that clients should invoke commit manually.
     * Clients must invoke {@link Sink#flush()} before commit.
     * @param connection the shared JDBC connection object
     * @return the created writer
     * @throws IOException if I/O error was occurred while initializing the sink
     * @throws InterruptedException if interrupted while initializing the sink
     */
    Sink open(Connection connection) throws IOException, InterruptedException;

    /**
     * Represents an object sink of {@link JdbcOperationDriver}.
     * @since 0.4.0
     */
    interface Sink extends ObjectSink, InterruptibleIo {

        /**
         * Flushes this sink for commit the current transaction.
         * @return {@code true} if actually flushed, of {@code false} if there is no records to flush
         * @throws IOException if I/O error was occurred while flushing the sink
         * @throws InterruptedException if interrupted while flushing the sink
         */
        boolean flush() throws IOException, InterruptedException;
    }
}
