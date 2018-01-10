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
package com.asakusafw.dag.runtime.jdbc.basic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo.Closer;

/**
 * A basic implementation of {@link JdbcInputDriver}.
 * @since 0.4.0
 */
public class BasicJdbcInputDriver implements JdbcInputDriver {

    static final Logger LOG = LoggerFactory.getLogger(BasicJdbcInputDriver.class);

    private final String sql;

    private final Supplier<? extends ResultSetAdapter<?>> adapters;

    private final int fetchSize;

    /**
     * Creates a new instance.
     * @param sql the input query
     * @param adapters the result set adapter provider
     */
    public BasicJdbcInputDriver(String sql, Supplier<? extends ResultSetAdapter<?>> adapters) {
        this(sql, adapters, -1);
    }

    /**
     * Creates a new instance.
     * @param sql the input query
     * @param adapters the result set adapter provider
     * @param fetchSize the bulk fetch size, or {@code <= 0} if it is not specified
     */
    public BasicJdbcInputDriver(String sql, Supplier<? extends ResultSetAdapter<?>> adapters, int fetchSize) {
        Arguments.requireNonNull(sql);
        Arguments.requireNonNull(adapters);
        this.sql = sql;
        this.adapters = adapters;
        this.fetchSize = fetchSize;
    }

    @Override
    public List<? extends Partition> getPartitions(Connection connection) {
        return Collections.singletonList(conn -> open(conn, sql, adapters.get(), fetchSize));
    }

    /**
     * Creates a new reader which returns each object of query result.
     * @param connection the shared JDBC connection
     * @param sql the input query
     * @param adapter the result set adapter
     * @param fetchSize the bulk fetch size, or {@code <= 0} if it is not specified
     * @return the created reader
     * @throws IOException if I/O error was occurred while computing input partitions
     * @throws InterruptedException if interrupted while computing input partitions
     */
    public static ObjectReader open(
            Connection connection,
            String sql,
            ResultSetAdapter<?> adapter,
            int fetchSize) throws IOException, InterruptedException {
        Arguments.requireNonNull(connection);
        Arguments.requireNonNull(sql);
        Arguments.requireNonNull(adapter);
        Arguments.requireNonNull(fetchSize);
        LOG.debug("JDBC input: {}", sql); //$NON-NLS-1$
        try (Closer closer = new Closer()) {
            Statement statement = connection.createStatement();
            closer.add(JdbcUtil.wrap(statement::close));
            if (fetchSize > 0) {
                statement.setFetchSize(fetchSize);
            }
            ResultSet results = statement.executeQuery(sql);
            closer.add(JdbcUtil.wrap(results::close));

            return new BasicFetchCursor(results, adapter, closer.move());
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }
}
