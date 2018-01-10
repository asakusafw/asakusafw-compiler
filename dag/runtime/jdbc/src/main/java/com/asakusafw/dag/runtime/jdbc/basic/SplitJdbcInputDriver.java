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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo.Closer;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Splittable {@link JdbcInputDriver}.
 * @since 0.4.0
 */
public class SplitJdbcInputDriver implements JdbcInputDriver {

    static final Logger LOG = LoggerFactory.getLogger(SplitJdbcInputDriver.class);

    private static final int DELTA_SCALE = 3;

    private static final Set<Integer> SUPPORTED_TYPES = IntStream.builder()
            .add(java.sql.Types.TINYINT)
            .add(java.sql.Types.SMALLINT)
            .add(java.sql.Types.INTEGER)
            .add(java.sql.Types.BIGINT)
            .add(java.sql.Types.NUMERIC)
            .add(java.sql.Types.DECIMAL)
            .add(java.sql.Types.DATE)
            .add(java.sql.Types.TIMESTAMP)
            .build()
            .mapToObj(i -> i)
            .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));

    private static final BigDecimal TWO = BigDecimal.valueOf(2L);

    private final JdbcProfile profile;

    private final String tableName;

    private final List<String> columnNames;

    private final String splitColumnName;

    private final int splitCount;

    private final String condition;

    private final Supplier<? extends ResultSetAdapter<?>> adapters;

    /**
     * Creates a new instance.
     * @param profile the current profile
     * @param tableName the target table name
     * @param columnNames the target column names
     * @param condition the input condition (optional)
     * @param splitColumnName the split column name
     * @param splitCount the max split count ({@code must be >= 2})
     * @param adapters the result set adapter provider
     */
    public SplitJdbcInputDriver(
            JdbcProfile profile,
            String tableName,
            List<String> columnNames,
            String splitColumnName,
            int splitCount,
            String condition,
            Supplier<? extends ResultSetAdapter<?>> adapters) {
        Arguments.requireNonNull(profile);
        Arguments.requireNonNull(tableName);
        Arguments.requireNonNull(columnNames);
        Arguments.requireNonNull(splitColumnName);
        Arguments.require(splitCount >= 2);
        Arguments.requireNonNull(adapters);
        this.profile = profile;
        this.tableName = tableName;
        this.columnNames = Arguments.freeze(columnNames);
        this.splitColumnName = splitColumnName;
        this.splitCount = splitCount;
        this.condition = condition;
        this.adapters = adapters;
    }

    @Override
    public List<? extends JdbcInputDriver.Partition> getPartitions(
            Connection connection) throws IOException, InterruptedException {
        Optional<Stats> stats = getStats(connection);
        List<?> boundValues = stats
                .map(s -> computeBoundValues(s)) // for findbugs
                .orElse(Collections.emptyList());
        if (boundValues.isEmpty()) {
            int fetchSize = profile.getFetchSize().orElse(-1);
            String sql = JdbcUtil.getSelectStatement(tableName, columnNames, condition);
            return Collections.singletonList(conn -> BasicJdbcInputDriver.open(conn, sql, adapters.get(), fetchSize));
        }
        return buildPartitions(stats.get(), boundValues);
    }

    private Optional<Stats> getStats(Connection connection) throws IOException, InterruptedException {
        String sql = getStatsSql();
        LOG.debug("split stats: {}", sql); //$NON-NLS-1$
        try (Closer closer = new Closer()) {
            Statement statement = connection.createStatement();
            closer.add(JdbcUtil.wrap(statement::close));
            ResultSet rs = statement.executeQuery(sql);
            closer.add(JdbcUtil.wrap(rs::close));

            ResultSetMetaData meta = rs.getMetaData();
            int type = meta.getColumnType(1);
            int scale = meta.getScale(1);
            boolean nullable = meta.isNullable(1) != ResultSetMetaData.columnNoNulls;
            if (SUPPORTED_TYPES.contains(type) == false) {
                LOG.warn(MessageFormat.format(
                        "unsupported split column type: {1}:{2} ({0})",
                        type,
                        splitColumnName,
                        meta.getColumnTypeName(1)));
                return Optionals.empty();
            }
            Object min = null;
            Object max = null;
            if (rs.next()) {
                min = getValue(rs, type, 1);
                max = getValue(rs, type, 2);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("split stats: table={}, column={}:{}, range=[{}, {}]", new Object[] { //$NON-NLS-1$
                        tableName,
                        splitColumnName, meta.getColumnTypeName(1),
                        min, max,
                });
            }
            return Optionals.of(new Stats(type, nullable, scale, min, max));
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }

    private static Object getValue(ResultSet rs, int type, int index) throws SQLException {
        switch (type) {
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
            return rs.getBigDecimal(index);
        default:
            return rs.getObject(index);
        }
    }

    private String getStatsSql() {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT "); //$NON-NLS-1$
        buf.append(String.format("MIN(%1$s), MAX(%1$s)", splitColumnName)); //$NON-NLS-1$
        buf.append(" FROM "); //$NON-NLS-1$
        buf.append(tableName);
        if (condition != null) {
            buf.append(" WHERE "); //$NON-NLS-1$
            buf.append(condition);
        }
        return buf.toString();
    }

    private List<?> computeBoundValues(Stats stats) {
        if (stats.min == null || stats.max == null || stats.min.equals(stats.max)) {
            return Collections.emptyList();
        }
        assert splitCount >= 2;
        switch (stats.typeId) {
        case java.sql.Types.TINYINT:
            return computeBoundValues(((Number) stats.min).byteValue(), ((Number) stats.max).byteValue());
        case java.sql.Types.SMALLINT:
            return computeBoundValues(((Number) stats.min).shortValue(), ((Number) stats.max).shortValue());
        case java.sql.Types.INTEGER:
            return computeBoundValues(((Number) stats.min).intValue(), ((Number) stats.max).intValue());
        case java.sql.Types.BIGINT:
            return computeBoundValues(((Number) stats.min).longValue(), ((Number) stats.max).longValue());
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
            return computeBoundValues((BigDecimal) stats.min, (BigDecimal) stats.max, stats.scale);
        case java.sql.Types.DATE:
            return computeBoundValues((java.sql.Date) stats.min, (java.sql.Date) stats.max);
        case java.sql.Types.TIMESTAMP:
            return computeBoundValues((java.sql.Timestamp) stats.min, (java.sql.Timestamp) stats.max);
        default:
            throw new AssertionError();
        }
    }

    private List<Byte> computeBoundValues(byte min, byte max) {
        return computeBoundValues(BigDecimal.valueOf(min), BigDecimal.valueOf(max), 0).stream()
                .map(BigDecimal::byteValueExact)
                .collect(Collectors.toList());
    }

    private List<Short> computeBoundValues(short min, short max) {
        return computeBoundValues(BigDecimal.valueOf(min), BigDecimal.valueOf(max), 0).stream()
                .map(BigDecimal::shortValueExact)
                .collect(Collectors.toList());
    }

    private List<Integer> computeBoundValues(int min, int max) {
        return computeBoundValues(BigDecimal.valueOf(min), BigDecimal.valueOf(max), 0).stream()
                .map(BigDecimal::intValueExact)
                .collect(Collectors.toList());
    }

    private List<Long> computeBoundValues(long min, long max) {
        return computeBoundValues(BigDecimal.valueOf(min), BigDecimal.valueOf(max), 0).stream()
                .map(BigDecimal::longValueExact)
                .collect(Collectors.toList());
    }

    private List<java.sql.Date> computeBoundValues(java.sql.Date min, java.sql.Date max) {
        Calendar buf = Calendar.getInstance();
        return computeBoundValues(
                    BigDecimal.valueOf(JdbcUtil.fromDate(min)),
                    BigDecimal.valueOf(JdbcUtil.fromDate(max)), 0).stream()
                .map(BigDecimal::intValueExact)
                .map(v -> JdbcUtil.toDate(v, buf))
                .collect(Collectors.toList());
    }

    private List<java.sql.Timestamp> computeBoundValues(java.sql.Timestamp min, java.sql.Timestamp max) {
        Calendar buf = Calendar.getInstance();
        return computeBoundValues(
                    BigDecimal.valueOf(JdbcUtil.fromTimestamp(min)),
                    BigDecimal.valueOf(JdbcUtil.fromTimestamp(max)), 0).stream()
                .map(BigDecimal::longValueExact)
                .map(v -> JdbcUtil.toTimestamp(v, buf))
                .collect(Collectors.toList());
    }

    private List<BigDecimal> computeBoundValues(BigDecimal min, BigDecimal max, int scale) {
        if (min.compareTo(max) >= 0) {
            return Collections.emptyList();
        }
        /*
         * BOUND(i) = MIN + DELTA * (i + 0.5), i = 0..COUNT-2,
         *   WHERE DELTA = (MAX - MIN) / (COUNT - 1)
         */
        BigDecimal delta = max.subtract(min)
                .divide(BigDecimal.valueOf(splitCount - 1), dscale(scale + DELTA_SCALE), BigDecimal.ROUND_DOWN);
        BigDecimal current = min.add(delta.divide(TWO, BigDecimal.ROUND_DOWN)).setScale(dscale(scale + DELTA_SCALE));
        List<BigDecimal> results = new ArrayList<>();
        add(results, current, scale);
        for (int i = 0, n = splitCount - 2; i <= n; i++) {
            BigDecimal next = current.add(delta);
            add(results, current, scale);
            current = next;
        }
        Invariants.require(results.size() <= splitCount - 1);
        return results;
    }

    private static int dscale(int sqlScale) {
        return sqlScale;
    }

    private static void add(List<BigDecimal> results, BigDecimal current, int scale) {
        BigDecimal value = current.setScale(dscale(scale), BigDecimal.ROUND_HALF_UP);
        if (results.isEmpty() || value.equals(results.get(results.size() - 1)) == false) {
            results.add(value);
        }
    }

    private List<? extends JdbcInputDriver.Partition> buildPartitions(Stats stats, List<?> boundValues) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("split {} into {} partitions by {}: {}", new Object[] { //$NON-NLS-1$
                    tableName,
                    boundValues.size() + 1,
                    splitColumnName,
                    boundValues,
            });
        }
        List<Partition> results = new ArrayList<>();
        results.add(toLowerPartition(stats, boundValues.get(0)));
        for (int i = 0, n = boundValues.size() - 1; i < n; i++) {
            Object lower = boundValues.get(i + 0);
            Object upper = boundValues.get(i + 1);
            results.add(toBodyPartition(stats, lower, upper));
        }
        results.add(toUpperPartition(stats, boundValues.get(boundValues.size() - 1)));
        return results;
    }

    private Partition toLowerPartition(Stats stats, Object value) {
        StringBuilder buf = getQueryPrefix();
        if (stats.nullable) {
            buf.append("("); //$NON-NLS-1$
            buf.append(splitColumnName);
            buf.append(" IS NULL "); //$NON-NLS-1$
            buf.append("OR "); //$NON-NLS-1$
        }
        buf.append(splitColumnName);
        buf.append(" < ?"); //$NON-NLS-1$
        if (stats.nullable) {
            buf.append(")"); //$NON-NLS-1$
        }
        return new Partition(buf.toString(), Collections.singletonList(value), stats.typeId, adapters.get());
    }

    private Partition toBodyPartition(Stats stats, Object lower, Object upper) {
        StringBuilder buf = getQueryPrefix();
        if (stats.nullable) {
            buf.append(splitColumnName);
            buf.append(" IS NOT NULL "); //$NON-NLS-1$
            buf.append("AND "); //$NON-NLS-1$
        }
        buf.append("? <= "); //$NON-NLS-1$
        buf.append(splitColumnName);
        buf.append(" AND "); //$NON-NLS-1$
        buf.append(splitColumnName);
        buf.append(" < ?"); //$NON-NLS-1$
        return new Partition(buf.toString(), Arrays.asList(lower, upper), stats.typeId, adapters.get());
    }

    private Partition toUpperPartition(Stats stats, Object value) {
        StringBuilder buf = getQueryPrefix();
        if (stats.nullable) {
            buf.append(splitColumnName);
            buf.append(" IS NOT NULL "); //$NON-NLS-1$
            buf.append("AND "); //$NON-NLS-1$
        }
        buf.append("? <= "); //$NON-NLS-1$
        buf.append(splitColumnName);
        return new Partition(buf.toString(), Collections.singletonList(value), stats.typeId, adapters.get());
    }

    private StringBuilder getQueryPrefix() {
        StringBuilder buf = new StringBuilder();
        buf.append(JdbcUtil.getSelectStatement(tableName, columnNames));
        if (condition == null) {
            buf.append(" WHERE "); //$NON-NLS-1$
        } else {
            buf.append(" WHERE ").append(condition).append(" AND "); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return buf;
    }

    private static class Stats {

        final int typeId;

        final boolean nullable;

        final int scale;

        final Object min;

        final Object max;

        Stats(int typeId, boolean nullable, int scale, Object min, Object max) {
            this.typeId = typeId;
            this.nullable = nullable;
            this.scale = scale;
            this.min = min;
            this.max = max;
        }
    }

    private static class Partition implements JdbcInputDriver.Partition {

        private final String sql;

        private final List<?> arguments;

        private final int argumentType;

        private final ResultSetAdapter<?> adapter;

        Partition(String sql, List<?> arguments, int argumentType, ResultSetAdapter<?> adapter) {
            this.sql = sql;
            this.arguments = arguments;
            this.argumentType = argumentType;
            this.adapter = adapter;
        }

        @Override
        public ObjectReader open(Connection connection) throws IOException, InterruptedException {
            LOG.debug("split SELECT: {} :: {}", sql, arguments); //$NON-NLS-1$
            try (Closer closer = new Closer()) {
                PreparedStatement statement = connection.prepareStatement(sql);
                closer.add(JdbcUtil.wrap(statement::close));
                for (int i = 0, n = arguments.size(); i < n; i++) {
                    Object argument = arguments.get(i);
                    statement.setObject(i + 1, argument, argumentType);
                }
                ResultSet rs = statement.executeQuery();
                closer.add(JdbcUtil.wrap(rs::close));
                return new BasicFetchCursor(rs, adapter, closer.move());
            } catch (SQLException e) {
                throw JdbcUtil.wrap(e);
            }
        }
    }
}
