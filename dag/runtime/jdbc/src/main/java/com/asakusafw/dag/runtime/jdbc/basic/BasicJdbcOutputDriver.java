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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.runtime.jdbc.JdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.PreparedStatementAdapter;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo.Closer;

/**
 * A basic implementation of {@link JdbcOutputDriver}.
 * @since 0.4.0
 */
public class BasicJdbcOutputDriver implements JdbcOutputDriver {

    static final Logger LOG = LoggerFactory.getLogger(BasicJdbcOutputDriver.class);

    private final String sql;

    private final Supplier<? extends PreparedStatementAdapter<?>> adapters;

    /**
     * Creates a new instance.
     * @param sql the insert statement with place-holders
     * @param adapters the prepared statement adapter provider
     */
    public BasicJdbcOutputDriver(String sql, Supplier<? extends PreparedStatementAdapter<?>> adapters) {
        Arguments.requireNonNull(sql);
        Arguments.requireNonNull(adapters);
        this.sql = sql;
        this.adapters = adapters;
    }

    @Override
    public JdbcOutputDriver.Sink open(Connection connection) throws IOException, InterruptedException {
        LOG.debug("JDBC output: {}", sql); //$NON-NLS-1$
        try (Closer closer = new Closer()) {
            PreparedStatement statement = connection.prepareStatement(sql);
            closer.add(JdbcUtil.wrap(statement::close));
            return new Sink(statement, adapters.get(), closer.move());
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }

    private static class Sink implements JdbcOutputDriver.Sink {

        private final PreparedStatement statement;

        private final PreparedStatementAdapter<Object> adapter;

        private final Closer resource;

        private boolean dirty;

        @SuppressWarnings("unchecked")
        Sink(PreparedStatement statement, PreparedStatementAdapter<?> adapter, Closer resource) {
            Arguments.requireNonNull(statement);
            Arguments.requireNonNull(adapter);
            Arguments.requireNonNull(resource);
            this.adapter = (PreparedStatementAdapter<Object>) adapter;
            this.statement = statement;
            this.resource = resource;
        }

        @Override
        public void putObject(Object object) throws IOException, InterruptedException {
            try {
                adapter.drive(statement, object);
                statement.addBatch();
                dirty = true;
            } catch (SQLException e) {
                throw JdbcUtil.wrap(e);
            }
        }

        @Override
        public boolean flush() throws IOException, InterruptedException {
            if (dirty) {
                dirty = false;
                try {
                    statement.executeBatch();
                } catch (SQLException e) {
                    throw JdbcUtil.wrap(e);
                }
                return true;
            }
            return false;
        }

        @Override
        public void close() throws IOException, InterruptedException {
            resource.close();
        }
    }
}
