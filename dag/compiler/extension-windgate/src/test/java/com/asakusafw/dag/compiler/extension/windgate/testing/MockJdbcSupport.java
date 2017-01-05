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
package com.asakusafw.dag.compiler.extension.windgate.testing;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.windgate.core.vocabulary.DataModelJdbcSupport;

/**
 * A {@link DataModelJdbcSupport} for {@link MockDataModel}.
 */
public class MockJdbcSupport implements DataModelJdbcSupport<MockDataModel> {

    /**
     * The column names.
     */
    public static final List<String> COLUMNS = Collections.unmodifiableList(Arrays.asList(new String[] {
            "M_KEY",
            "M_SORT",
            "M_VALUE",
    }));

    private static final Map<String, String> COLUMN_MAP = COLUMNS.stream()
            .collect(Collectors.collectingAndThen(
                    Collectors.toMap(
                            s -> s, // NOTE: some compilers cannot infer Function.identity()
                            s -> s.substring(2).toLowerCase(Locale.ENGLISH)),
                    Collections::unmodifiableMap));

    /**
     * Returns a DDL for the table.
     * @param tableName the table name
     * @return the DDL
     */
    public static String ddl(String tableName) {
        return String.format("CREATE TABLE %s(M_KEY INT, M_SORT DECIMAL(18,2), M_VALUE VARCHAR(256))", tableName);
    }

    /**
     * Returns a SELECT query for the table.
     * @param tableName the table name
     * @return the query
     */
    public static String select(String tableName) {
        return String.format("SELECT M_KEY, M_SORT, M_VALUE FROM %s ORDER BY M_KEY", tableName);
    }

    /**
     * Returns a INSERT statement for the table.
     * @param tableName the table name
     * @return the DML
     */
    public static String insert(String tableName) {
        return String.format("INSERT INTO %s(M_KEY, M_SORT, M_VALUE) VALUES(?, ?, ?)", tableName);
    }

    @Override
    public Class<MockDataModel> getSupportedType() {
        return MockDataModel.class;
    }

    @Override
    public Map<String, String> getColumnMap() {
        return COLUMN_MAP;
    }

    @Override
    public boolean isSupported(List<String> columnNames) {
        return columnNames.equals(COLUMNS);
    }

    @Override
    public DataModelResultSet<MockDataModel> createResultSetSupport(
            ResultSet rs, List<String> columnNames) {
        return new DataModelResultSet<MockDataModel>() {
            @SuppressWarnings("deprecation")
            @Override
            public boolean next(MockDataModel object) throws SQLException {
                if (!rs.next()) {
                    return false;
                }
                int key = rs.getInt(1);
                if (rs.wasNull()) {
                    object.getKeyOption().setNull();
                } else {
                    object.getKeyOption().modify(key);
                }
                BigDecimal sort = rs.getBigDecimal(2);
                if (rs.wasNull()) {
                    object.getSortOption().setNull();
                } else {
                    object.getSortOption().modify(sort);
                }
                String value = rs.getString(3);
                if (rs.wasNull()) {
                    object.getValueOption().setNull();
                } else {
                    object.getValueOption().modify(value);
                }
                return true;
            }
        };
    }

    @Override
    public DataModelPreparedStatement<MockDataModel> createPreparedStatementSupport(
            PreparedStatement ps, List<String> columnNames) {
        return new DataModelPreparedStatement<MockDataModel>() {
            @Override
            public void setParameters(MockDataModel object) throws SQLException {
                if (object.getKeyOption().isNull()) {
                    ps.setNull(1, Types.INTEGER);
                } else {
                    ps.setInt(1, object.getKey());
                }
                if (object.getSortOption().isNull()) {
                    ps.setNull(2, Types.DECIMAL);
                } else {
                    ps.setBigDecimal(2, object.getSort());
                }
                if (object.getValueOption().isNull()) {
                    ps.setNull(3, Types.VARCHAR);
                } else {
                    ps.setString(3, object.getValue());
                }
            }
        };
    }
}
