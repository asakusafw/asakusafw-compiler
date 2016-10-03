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
package com.asakusafw.dag.runtime.jdbc.basic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import com.asakusafw.dag.runtime.jdbc.ConnectionPool;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * A simple implementation of {@link ConnectionPool}.
 * This implementation always closes connections in released connection handles.
 * @since 0.4.0
 */
public class SimpleConnectionPool implements ConnectionPool {

    private final Driver driver;

    private final String url;

    private final Properties properties;

    private final int size;

    private final Semaphore semaphore;

    /**
     * Creates a new instance.
     * @param driver the JDBC driver instance (nullable)
     * @param url the JDBC URL
     * @param properties the JDBC properties
     * @param maxConnections the number of max connections
     */
    public SimpleConnectionPool(Driver driver, String url, Map<String, String> properties, int maxConnections) {
        Arguments.requireNonNull(url);
        Arguments.requireNonNull(properties);
        Arguments.require(maxConnections >= 1);
        this.driver = driver;
        this.url = url;
        this.properties = new Properties();
        this.properties.putAll(properties);
        this.size = maxConnections;
        this.semaphore = new Semaphore(maxConnections);
    }

    /**
     * Creates a new instance.
     * @param url the JDBC URL
     * @param properties the JDBC properties
     * @param maxConnections the number of max connections
     */
    public SimpleConnectionPool(String url, Map<String, String> properties, int maxConnections) {
        this(null, url, properties, maxConnections);
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public OptionalInt size() {
        return OptionalInt.of(size);
    }

    @Override
    public Handle acquire() throws IOException, InterruptedException {
        semaphore.acquire();
        boolean success = false;
        try {
            Connection connection = acquire0();
            connection.setAutoCommit(false);
            success = true;
            return new Handle(connection);
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        } finally {
            if (success == false) {
                semaphore.release();
            }
        }
    }

    void release(Connection connection) throws IOException {
        try {
            try {
                if (connection.isClosed() == false && connection.getAutoCommit() == false) {
                    connection.rollback();
                }
            } finally {
                connection.close();
            }
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        } finally {
            semaphore.release();
        }
    }

    private Connection acquire0() throws SQLException {
        if (driver != null) {
            return driver.connect(url, properties);
        } else {
            return DriverManager.getConnection(url, properties);
        }
    }

    @Override
    public void close() {
        return;
    }

    /**
     * Provides {@link SimpleConnectionPool} instance.
     * @since 0.4.0
     */
    public static class Provider implements ConnectionPool.Provider {

        @Override
        public ConnectionPool newInstance(String url, Map<String, String> properties, int maxConnections) {
            return new SimpleConnectionPool(url, properties, maxConnections);
        }
    }

    private class Handle implements ConnectionPool.Handle {

        private final Connection connection;

        private final AtomicBoolean closed = new AtomicBoolean();

        Handle(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Connection getConnection() throws IOException, InterruptedException {
            return connection;
        }

        @Override
        public void close() throws IOException, InterruptedException {
            if (closed.compareAndSet(false, true)) {
                SimpleConnectionPool.this.release(connection);
            }
        }
    }
}
