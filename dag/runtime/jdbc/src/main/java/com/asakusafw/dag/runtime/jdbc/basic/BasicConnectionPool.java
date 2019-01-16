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
package com.asakusafw.dag.runtime.jdbc.basic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.runtime.jdbc.ConnectionPool;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * A basic implementation of {@link ConnectionPool}.
 * @since 0.4.0
 */
public class BasicConnectionPool implements ConnectionPool {

    static final Logger LOG = LoggerFactory.getLogger(BasicConnectionPool.class);

    private final Driver driver;

    private final String url;

    private final Properties properties;

    private final int size;

    private final Semaphore semaphore;

    private final Queue<Connection> cached = new ConcurrentLinkedQueue<>();

    private volatile boolean poolClosed = false;

    /**
     * Creates a new instance.
     * @param driver the JDBC driver instance (nullable)
     * @param url the JDBC URL
     * @param properties the JDBC properties
     * @param maxConnections the number of max connections
     */
    public BasicConnectionPool(Driver driver, String url, Map<String, String> properties, int maxConnections) {
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
    public BasicConnectionPool(String url, Map<String, String> properties, int maxConnections) {
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

    /**
     * Returns the rest permissions (only for testing).
     * @return the rest permissions
     */
    public int rest() {
        return semaphore.availablePermits();
    }

    @Override
    public ConnectionPool.Handle acquire() throws IOException, InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("acquiring connection from pool: {}/{} ({})", rest(), size, cached.size()); //$NON-NLS-1$
        }
        semaphore.acquire();
        boolean success = false;
        try (Closer closer = new Closer()) {
            Connection connection = acquire0();
            closer.add(JdbcUtil.wrap(connection::close));
            connection.clearWarnings();
            connection.setAutoCommit(false);
            success = true;
            closer.keep();
            return new Handle(connection);
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        } finally {
            if (success == false) {
                semaphore.release();
            }
        }
    }

    private Connection acquire0() throws IOException {
        try {
            while (true) {
                if (poolClosed) {
                    throw new IOException("connection poll has been already closed");
                }
                Connection connection = cached.poll();
                if (connection != null) {
                    if (connection.isClosed()) {
                        connection.close();
                        continue;
                    } else {
                        return connection;
                    }
                } else {
                    return open();
                }
            }
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }

    private Connection open() throws SQLException {
        LOG.debug("opening connection: {}", url); //$NON-NLS-1$
        if (driver != null) {
            return driver.connect(url, properties);
        } else {
            return DriverManager.getConnection(url, properties);
        }
    }

    void release(Connection connection) throws IOException, InterruptedException {
        if (connection == null) {
            return;
        }
        try (Closer closer = new Closer()) {
            closer.add(JdbcUtil.wrap(connection::close));
            if (connection.isClosed() == false) {
                if (connection.getAutoCommit() == false) {
                    connection.rollback();
                }
                cached.add(connection);
                if (poolClosed == false) {
                    closer.keep();
                }
            }
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug("releasing connection into pool: {}/{} ({})", rest(), size, cached.size()); //$NON-NLS-1$
            }
            semaphore.release();
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        poolClosed = true;
        try (Closer closer = new Closer()) {
            while (true) {
                Connection connection = cached.poll();
                if (connection == null) {
                    break;
                } else {
                    closer.add(JdbcUtil.wrap(connection::close));
                }
            }
        }
    }

    /**
     * Provides {@link BasicConnectionPool} instance.
     * @since 0.4.0
     */
    public static class Provider implements ConnectionPool.Provider {

        @Override
        public ConnectionPool newInstance(String url, Map<String, String> properties, int maxConnections) {
            return new BasicConnectionPool(url, properties, maxConnections);
        }
    }

    private class Handle implements ConnectionPool.Handle {

        private final AtomicReference<Connection> connection;

        Handle(Connection connection) {
            Invariants.requireNonNull(connection);
            this.connection = new AtomicReference<>(connection);
        }

        @Override
        public Connection getConnection() throws IOException, InterruptedException {
            return Optionals.of(connection.get()).orElseThrow(IllegalStateException::new);
        }

        @Override
        public void close() throws IOException, InterruptedException {
            BasicConnectionPool.this.release(connection.getAndSet(null));
        }
    }
}
