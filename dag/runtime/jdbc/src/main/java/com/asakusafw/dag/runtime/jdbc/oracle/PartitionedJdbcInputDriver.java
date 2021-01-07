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
package com.asakusafw.dag.runtime.jdbc.oracle;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.OptionalDouble;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.utils.common.InterruptibleIo.Closer;

/**
 * {@link JdbcInputDriver} for partitioned tables.
 * @since 0.4.2
 */
public class PartitionedJdbcInputDriver implements JdbcInputDriver {

    static final Logger LOG = LoggerFactory.getLogger(PartitionedJdbcInputDriver.class);

    final JdbcProfile profile;

    final String tableName;

    final List<String> columnNames;

    final String condition;

    final Supplier<? extends ResultSetAdapter<?>> adapters;

    /**
     * Creates a new instance.
     * @param profile the current profile
     * @param tableName the target table name
     * @param columnNames the target column names
     * @param condition the input condition (optional)
     * @param adapters the result set adapter provider
     */
    public PartitionedJdbcInputDriver(
            JdbcProfile profile,
            String tableName,
            List<String> columnNames,
            String condition,
            Supplier<? extends ResultSetAdapter<?>> adapters) {
        this.profile = profile;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.condition = condition;
        this.adapters = adapters;
    }

    @Override
    public List<? extends JdbcInputDriver.Partition> getPartitions(
            Connection connection) throws IOException, InterruptedException {
        List<Partition> partitions = collectPartitions(connection);
        if (partitions.isEmpty()) {
            throw new IOException(MessageFormat.format(
                    "there are no available partitions in table \"{0}\"",
                    tableName));
        }
        return partitions;
    }

    private List<Partition> collectPartitions(Connection connection) throws IOException, InterruptedException {
        List<Partition> partitions = collectPartitions(connection, true);
        if (partitions.isEmpty() == false) {
            return partitions;
        }
        return collectPartitions(connection, false);
    }

    private List<Partition> collectPartitions(
            Connection connection,
            boolean caseSensitive) throws IOException, InterruptedException {
        String sql = buildPartitionInfoStatement(caseSensitive);
        LOG.debug("collect partitions: {}", sql);
        try (Closer closer = new Closer()) {
            Statement statement = connection.createStatement();
            closer.add(JdbcUtil.wrap(statement::close));
            ResultSet rs = statement.executeQuery(sql);
            closer.add(JdbcUtil.wrap(rs::close));

            List<Partition> results = new ArrayList<>();
            while (rs.next()) {
                String partitionName = rs.getString(1);
                double rows = rs.getDouble(2);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("partition status: table={}, partition={}, rows={}",
                            tableName, partitionName, rows);
                }
                if (Double.isNaN(rows) || rows < 1) {
                    rows = 1.0;
                }
                results.add(new Partition(partitionName, rows));
            }
            return results;
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }

    String buildPartitionInfoStatement(boolean caseSensitive) {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT PARTITION_NAME, NUM_ROWS")
            .append(" FROM ALL_TAB_PARTITIONS")
            .append(" WHERE ")
            .append(caseSensitive ? "TABLE_NAME" : "UPPER(TABLE_NAME)")
            .append(" = ")
            .append(OracleSyntax.quoteLiteral(caseSensitive ? tableName : tableName.toUpperCase(Locale.ENGLISH)));
        return buf.toString();
    }

    String buildSelectStatement(String partitionName) {
        StringBuilder buf = new StringBuilder();
        buf.append(JdbcUtil.getSelectStatement(tableName, columnNames));
        buf.append(" PARTITION(").append(OracleSyntax.quoteName(partitionName)).append(")"); //$NON-NLS-1$ //$NON-NLS-2$
        if (condition != null) {
            buf.append(" WHERE ").append(condition); //$NON-NLS-1$
        }
        return buf.toString();
    }

    private final class Partition implements JdbcInputDriver.Partition {

        private final String name;

        private final double size;

        Partition(String name, double size) {
            this.name = name;
            this.size = size;
        }

        @Override
        public ObjectReader open(Connection connection) throws IOException, InterruptedException {
            String sql = buildSelectStatement(name);
            int fetchSize = profile.getFetchSize().orElse(-1);
            return BasicJdbcInputDriver.open(connection, sql, adapters.get(), fetchSize);
        }

        @Override
        public String toString() {
            return String.format("Partition(table=%s, name=%s, size=%f)", tableName, name, size);
        }

        @Override
        public OptionalDouble getEsitimatedRowCount() {
            return OptionalDouble.of(size);
        }
    }
}
