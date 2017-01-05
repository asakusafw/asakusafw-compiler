/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.runtime.jdbc.util;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.RunnableWithException;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.DateUtil;

/**
 * Utilities about JDBC.
 * @since 0.4.0
 */
public final class JdbcUtil {

    static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);

    private JdbcUtil() {
        return;
    }

    /**
     * Returns an {@link IOException} object which wraps a {@link SQLException}.
     * @param exception the original exception
     * @return the wrapped exception
     */
    public static IOException wrap(SQLException exception) {
        int depth = 0;
        for (SQLException e = exception; e != null; e = e.getNextException()) {
            LOG.error("[{}]", depth++, e); //$NON-NLS-1$
        }
        return new IOException(exception);
    }

    /**
     * Returns an {@link InterruptibleIo} operation which wraps an action with {@link SQLException}.
     * @param action the original action
     * @return the wrapped operation
     */
    public static InterruptibleIo wrap(RunnableWithException<SQLException> action) {
        return () -> {
            try {
                action.run();
            } catch (SQLException e) {
                throw wrap(e);
            }
        };
    }

    /**
     * Returns a basic select statement.
     * @param tableName the target table name
     * @param columnNames the column names
     * @return the build statement
     */
    public static String getSelectStatement(String tableName, List<String> columnNames) {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT "); //$NON-NLS-1$
        buf.append(String.join(",", columnNames)); //$NON-NLS-1$
        buf.append(" FROM "); //$NON-NLS-1$
        buf.append(tableName);
        return buf.toString();
    }

    /**
     * Returns a basic select statement.
     * @param tableName the target table name
     * @param columnNames the column names
     * @param condition the condition expression (nullable)
     * @return the build statement
     */
    public static String getSelectStatement(String tableName, List<String> columnNames, String condition) {
        String body = getSelectStatement(tableName, columnNames);
        if (condition == null) {
            return body;
        } else {
            return new StringBuilder().append(body).append(" WHERE ").append(condition).toString(); //$NON-NLS-1$
        }
    }

    /**
     * Returns a basic insert statement.
     * @param tableName the target table name
     * @param columnNames the column names
     * @return the built statement
     */
    public static String getInsertStatement(String tableName, List<String> columnNames) {
        StringBuilder buf = new StringBuilder();
        buf.append("INSERT "); //$NON-NLS-1$
        buf.append("INTO "); //$NON-NLS-1$
        buf.append(tableName);
        buf.append(" ("); //$NON-NLS-1$
        buf.append(String.join(",", columnNames)); //$NON-NLS-1$
        buf.append(") "); //$NON-NLS-1$
        buf.append("VALUES "); //$NON-NLS-1$
        buf.append("("); //$NON-NLS-1$
        buf.append(String.join(",", placeholders(columnNames.size()))); //$NON-NLS-1$
        buf.append(")"); //$NON-NLS-1$
        return buf.toString();
    }

    /**
     * Returns a basic delete statement.
     * @param tableName the target table name
     * @param condition the condition expression (nullable)
     * @return the build statement
     */
    public static String getDeleteStatement(String tableName, String condition) {
        StringBuilder buf = new StringBuilder();
        buf.append("DELETE "); //$NON-NLS-1$
        buf.append("FROM "); //$NON-NLS-1$
        buf.append(tableName);
        if (condition != null) {
            buf.append(" WHERE ").append(condition); //$NON-NLS-1$
        }
        return buf.toString();
    }

    /**
     * Returns a basic truncate statement.
     * @param tableName the target table name
     * @return the build statement
     */
    public static String getTruncateStatement(String tableName) {
        StringBuilder buf = new StringBuilder();
        buf.append("TRUNCATE "); //$NON-NLS-1$
        buf.append("TABLE "); //$NON-NLS-1$
        buf.append(tableName);
        return buf.toString();
    }

    private static List<String> placeholders(int count) {
        return Collections.nCopies(count, "?"); //$NON-NLS-1$
    }

    /**
     * Returns SQL Date object from the Asakusa Date representation (elapsed days from epoch).
     * @param value the Asakusa representation
     * @param calendarBuffer the calendar buffer
     * @return created SQL value
     */
    public static java.sql.Date toDate(int value, java.util.Calendar calendarBuffer) {
        DateUtil.setDayToCalendar(value, calendarBuffer);
        return new java.sql.Date(calendarBuffer.getTimeInMillis());
    }

    /**
     * Returns Asakusa Date representation from the SQL Date object.
     * @param date the SQL value
     * @return the Asakusa representation
     */
    public static int fromDate(java.sql.Date date) {
        return DateUtil.getDayFromDate(date);
    }

    /**
     * Returns SQL Timestamp object from the Asakusa DateTime representation (elapsed days from epoch).
     * @param value the Asakusa representation
     * @param calendarBuffer the calendar buffer
     * @return created SQL value
     */
    public static java.sql.Timestamp toTimestamp(long value, java.util.Calendar calendarBuffer) {
        DateUtil.setSecondToCalendar(value, calendarBuffer);
        return new java.sql.Timestamp(calendarBuffer.getTimeInMillis());
    }

    /**
     * Returns Asakusa DateTime representation from the SQL Timestamp object.
     * @param timestamp the SQL value
     * @return the Asakusa representation
     */
    public static long fromTimestamp(java.sql.Timestamp timestamp) {
        return DateUtil.getSecondFromDate(timestamp);
    }

    /**
     * Sets a {@link Date} value to placeholder.
     * @param statement the target statement
     * @param index the placeholder index
     * @param value the target value
     * @param calendarBuffer a calendar buffer
     * @throws SQLException if error was occurred
     */
    public static void setParameter(
            PreparedStatement statement,
            int index,
            Date value,
            java.util.Calendar calendarBuffer) throws SQLException {
        statement.setDate(index, toDate(value.getElapsedDays(), calendarBuffer), calendarBuffer);
    }

    /**
     * Sets a {@link Date} value to placeholder.
     * @param statement the target statement
     * @param index the placeholder index
     * @param value the target value
     * @param calendarBuffer a calendar buffer
     * @throws SQLException if error was occurred
     */
    public static void setParameter(
            PreparedStatement statement,
            int index,
            DateTime value,
            java.util.Calendar calendarBuffer) throws SQLException {
        DateUtil.setSecondToCalendar(value.getElapsedSeconds(), calendarBuffer);
        statement.setTimestamp(index, toTimestamp(value.getElapsedSeconds(), calendarBuffer), calendarBuffer);
    }
}
