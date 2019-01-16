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
package com.asakusafw.dag.runtime.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.util.Map;
import java.util.OptionalInt;

import com.asakusafw.lang.utils.common.InterruptibleIo;

/**
 * An abstract super interface of JDBC connection pools.
 * @since 0.4.0
 */
public interface ConnectionPool extends InterruptibleIo {

    /**
     * Returns the JDBC URL for connections.
     * @return the JDBC URL
     */
    String getUrl();

    /**
     * Returns the max number of connections in this pool.
     * @return the max number of connections
     */
    default OptionalInt size() {
        return OptionalInt.empty();
    }

    /**
     * Returns a handle of the connection pool entry.
     * This may block until a handle is available.
     * The handle will roll-back uncommitted transactions when the handle will be closed.
     * @return the acquired handle
     * @throws IOException if I/O error was occurred while acquiring a handle
     * @throws InterruptedException if interrupted while acquiring a handle
     */
    Handle acquire() throws IOException, InterruptedException;

    /**
     * Provides {@link ConnectionPool} instance.
     * Each {@link ConnectionPool} should have a nested {@code Provider} class which implements this interface.
     * @since 0.4.0
     */
    @FunctionalInterface
    interface Provider {

        /**
         * Creates a new instance.
         * @param url the JDBC URL
         * @param properties the JDBC properties
         * @param maxConnections the number of max connections
         * @return the created instance
         */
        ConnectionPool newInstance(String url, Map<String, String> properties, int maxConnections);
    }

    /**
     * JDBC connection handle for {@link ConnectionPool}.
     * @since 0.4.0
     */
    interface Handle extends InterruptibleIo {

        /**
         * Returns the handled connection.
         * Clients should not close the underlying connection manually.
         * @return the connection
         * @throws IOException if I/O error was occurred while obtaining the handled connection
         * @throws InterruptedException if interrupted while obtaining the handled connection
         */
        Connection getConnection() throws IOException, InterruptedException;
    }
}
