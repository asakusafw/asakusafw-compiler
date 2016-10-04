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
package com.asakusafw.dag.compiler.extension.windgate.testing;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.asakusafw.dag.runtime.jdbc.testing.H2Resource;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.windgate.core.vocabulary.DataModelJdbcSupport;
import com.asakusafw.windgate.core.vocabulary.DataModelJdbcSupport.DataModelPreparedStatement;
import com.asakusafw.windgate.core.vocabulary.DataModelJdbcSupport.DataModelResultSet;

/**
 * Helper for testing with JDBC.
 */
public class JdbcTestHelper extends H2Resource {

    /**
     * Creates a new instance.
     * @param name the database name
     */
    public JdbcTestHelper(String name) {
        super(name);
    }

    /**
     * Select records.
     * @param <T> the data type
     * @param query the query
     * @param columnNames the column names
     * @param supportClass the JDBC support class
     * @param action the action
     */
    public <T> void select(
            String query,
            List<String> columnNames,
            Class<? extends DataModelJdbcSupport<T>> supportClass,
            Action<List<T>, ?> action) {
        try (Connection connection = open();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(query)) {
            DataModelJdbcSupport<T> support = supportClass.newInstance();
            DataModelResultSet<T> adapter = support.createResultSetSupport(rs, columnNames);
            List<T> results = new ArrayList<>();
            while (true) {
                T object = support.getSupportedType().newInstance();
                if (adapter.next(object) == false) {
                    break;
                }
                results.add(object);
            }
            action.perform(results);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public JdbcTestHelper with(String sql) {
        super.with(sql);
        return this;
    }

    @Override
    public JdbcTestHelper with(Action<? super Connection, ?> action) {
        super.with(action);
        return this;
    }

    /**
     * Adds records.
     * @param <T> the data type
     * @param statement the statement with placeholders
     * @param columnNames the column names
     * @param supportClass the JDBC support class
     * @param action the action
     */
    public <T> void insert(
            String statement,
            List<String> columnNames,
            Class<? extends DataModelJdbcSupport<T>> supportClass,
            Action<ModelOutput<T>, ?> action) {
        try (Connection connection = open();
                PreparedStatement ps = connection.prepareStatement(statement)) {
            DataModelJdbcSupport<T> support = supportClass.newInstance();
            DataModelPreparedStatement<T> adapter = support.createPreparedStatementSupport(ps, columnNames);
            try (ModelOutput<T> output = new ModelOutput<T>() {
                @Override
                public void write(T model) throws IOException {
                    try {
                        adapter.setParameters(model);
                        ps.execute();
                        ps.addBatch();
                    } catch (SQLException e) {
                        throw new IOException(e);
                    }
                }
                @Override
                public void close() throws IOException {
                    return;
                }
            }) {
                action.perform(output);
            }
            connection.commit();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
