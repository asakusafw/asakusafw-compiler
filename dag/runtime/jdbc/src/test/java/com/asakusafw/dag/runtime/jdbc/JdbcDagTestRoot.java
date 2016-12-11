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
package com.asakusafw.dag.runtime.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.Rule;
import org.junit.rules.ExternalResource;

import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.runtime.jdbc.basic.BasicConnectionPool;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcContext;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcEnvironment;
import com.asakusafw.dag.runtime.jdbc.testing.H2Resource;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.FunctionWithException;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.InterruptibleIo.Closer;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.runtime.util.VariableTable;

/**
 * A common utilities for testing JDBC DAG.
 */
public abstract class JdbcDagTestRoot {

    /**
     * Table name.
     */
    public static final String TABLE = "KSV";

    /**
     * Columns.
     */
    public static final List<String> COLUMNS =
            Collections.unmodifiableList(Arrays.asList("M_KEY", "M_SORT", "M_VALUE"));

    /**
     * Select statement.
     */
    public static final String SELECT = String.format("SELECT %s FROM %s",
            String.join(", ", COLUMNS),
            TABLE,
            COLUMNS.get(0));

    /**
     * DDL format.
     */
    public static final String DDL_FORMAT = "CREATE TABLE %s(M_KEY BIGINT NOT NULL, M_SORT DECIMAL(18,2), M_VALUE VARCHAR(256))";

    /**
     * H2 resource.
     */
    @Rule
    public final H2Resource h2 = new H2Resource("cp")
        .with(String.format(DDL_FORMAT, TABLE));

    /**
     * Closes resources.
     */
    @Rule
    public final ExternalResource autoClose = new ExternalResource() {
        @Override
        protected void after() {
            try {
                try {
                    for (BasicConnectionPool pool : pools) {
                        Invariants.require(pool.size().getAsInt() == pool.rest());
                    }
                } finally {
                    closer.close();
                }
            } catch (IOException | InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    };

    final Closer closer = new Closer();

    final List<BasicConnectionPool> pools = new ArrayList<>();

    final List<Consumer<JdbcProfile.Builder>> editors = new ArrayList<>();

    /**
     * Adds a resource to be closed after this test case.
     * @param <T> the resource type
     * @param resource the resource
     * @return the resource
     */
    public <T extends InterruptibleIo> T bless(T resource) {
        if (resource != null) {
            closer.add(resource);
            if (resource instanceof BasicConnectionPool) {
                pools.add((BasicConnectionPool) resource);
            }
        }
        return resource;
    }

    /**
     * Adds a profile editor.
     * @param editor the editor
     */
    public void edit(Consumer<JdbcProfile.Builder> editor) {
        this.editors.add(editor);
    }

    /**
     * Creates a new pool.
     * @param connections the max number of connections
     * @return the created connection pool
     */
    public BasicConnectionPool pool(int connections) {
        return bless(new BasicConnectionPool(h2.getJdbcUrl(), Collections.emptyMap(), connections));
    }

    /**
     * Creates a new environment.
     * @param profileNames the profile names
     * @return the environment
     */
    public JdbcEnvironment environment(String... profileNames) {
        List<JdbcProfile> profiles = new ArrayList<>();
        for (String name : profileNames) {
            profiles.add(profile0(name, pool(1)));
        }
        return new JdbcEnvironment(profiles);
    }

    /**
     * Run an action with context.
     * @param profileName the profile name
     * @param action the action
     */
    public void context(String profileName, Action<JdbcContext, ?> action) {
        context(profileName, Collections.emptyMap(), action);
    }

    /**
     * Run an action with context.
     * @param profileName the profile name
     * @param variables the variables
     * @param action the action
     */
    public void context(String profileName, Map<String, String> variables, Action<JdbcContext, ?> action) {
        VariableTable vs = new VariableTable();
        vs.defineVariables(variables);
        try {
            action.perform(new JdbcContext.Basic(environment(profileName), vs::parse));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Runs an action with profile.
     * @param profileName the profile name
     * @param action the action
     */
    public void profile(String profileName, Action<? super JdbcProfile, ?> action) {
        try (ConnectionPool pool = new BasicConnectionPool(h2.getJdbcUrl(), Collections.emptyMap(), 1)) {
            action.perform(profile0(profileName, pool));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private JdbcProfile profile0(String profileName, ConnectionPool pool) {
        JdbcProfile.Builder builder = new JdbcProfile.Builder(profileName);
        editors.forEach(e -> e.accept(builder));
        return builder.build(pool);
    }

    /**
     * Returns all records from {@code KSV} table.
     * @return the results
     * @throws SQLException if error was occurred
     */
    public List<KsvModel> select() throws SQLException {
        try (Connection c = h2.open()) {
            return select(c, TABLE);
        }
    }

    /**
     * Returns all records from {@code KSV} table.
     * @param connection the current connection
     * @param table the table name
     * @return the results
     * @throws SQLException if error was occurred
     */
    public List<KsvModel> select(Connection connection, String table) throws SQLException {
        String sql = String.format("SELECT M_KEY, M_SORT, M_VALUE FROM %s ORDER BY M_KEY, M_SORT", table);
        List<KsvModel> results = new ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                KsvModel record = new KsvModel();
                record.setKey(rs.getLong(1));
                record.setSort(rs.getBigDecimal(2));
                record.setValue(rs.getString(3));
                results.add(record);
            }
        }
        return results;
    }

    /**
     * Inserts a new record into {@code KSV} table.
     * @param key the key
     * @param sort the sort as string
     * @param value the value
     * @return the inserted record
     * @throws SQLException if error was occurred
     */
    public KsvModel insert(long key, String sort, String value) throws SQLException {
        try (Connection c = h2.open()) {
            return insert(c, key, sort, value);
        }
    }

    /**
     * Inserts a new record into {@code KSV} table.
     * @param model the model
     * @return the inserted record
     * @throws SQLException if error was occurred
     */
    public KsvModel insert(KsvModel model) throws SQLException {
        try (Connection c = h2.open()) {
            return insert(c, model.getKey(), model.getSort(), model.getValue());
        }
    }

    /**
     * Inserts a new record into {@code KSV} table.
     * @param connection the current connection
     * @param key the key
     * @param sort the sort as string
     * @param value the value
     * @return the inserted record
     * @throws SQLException if error was occurred
     */
    public KsvModel insert(Connection connection, long key, String sort, String value) throws SQLException {
        return insert(connection, key, sort == null ? null : new BigDecimal(sort), value);
    }

    private KsvModel insert(Connection connection, long key, BigDecimal sort, String value) throws SQLException {
        String sql = "INSERT INTO KSV(M_KEY, M_SORT, M_VALUE) VALUES(?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, key);
            statement.setBigDecimal(2, sort);
            statement.setString(3, value);
            statement.execute();
            connection.commit();
        }
        return new KsvModel(key, sort, value);
    }

    /**
     * Returns values from the input driver.
     * @param driver the input driver
     * @return the obtained value
     * @throws IOException if error was occurred
     * @throws InterruptedException if interrupted
     */
    public List<KsvModel> get(JdbcInputDriver driver) throws IOException, InterruptedException {
        List<KsvModel> results = new ArrayList<>();
        try (Connection conn = h2.open()) {
            for (JdbcInputDriver.Partition partition : driver.getPartitions(conn)) {
                results.addAll(get(partition));
            }
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
        return results;
    }

    /**
     * Returns values from the partition.
     * @param partition the partition
     * @return the obtained value
     * @throws IOException if error was occurred
     * @throws InterruptedException if interrupted
     */
    public List<KsvModel> get(JdbcInputDriver.Partition partition) throws IOException, InterruptedException {
        List<KsvModel> results = new ArrayList<>();
        try (Connection conn = h2.open();
                ObjectReader reader = partition.open(conn)) {
            while (reader.nextObject()) {
                results.add(new KsvModel((KsvModel) reader.getObject()));
            }
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
        results.sort((a, b) -> Long.compare(a.getKey(), b.getKey()));
        return results;
    }

    /**
     * Puts values into the output driver.
     * @param driver the output driver
     * @param values the values
     * @throws IOException if error was occurred
     * @throws InterruptedException if interrupted
     */
    public void put(JdbcOutputDriver driver, Object... values) throws IOException, InterruptedException {
        try (Connection conn = h2.open();
                JdbcOutputDriver.Sink sink = driver.open(conn)) {
            for (Object value : values) {
                sink.putObject(value);
            }
            sink.flush();
            conn.commit();
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }

    /**
     * Performs the operation driver.
     * @param driver the driver
     * @throws IOException if error was occurred
     * @throws InterruptedException if interrupted
     */
    public void perform(JdbcOperationDriver driver) throws IOException, InterruptedException {
        try (Connection conn = h2.open()) {
            driver.perform(conn);
            conn.commit();
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }

    /**
     * Executes an operation with JDBC connection.
     * @param <T> the result type
     * @param function the operation
     * @return the operation result
     */
    public <T> T connect(FunctionWithException<Connection, T, ?> function) {
        try (Connection conn = h2.open()) {
            T result = function.apply(conn);
            conn.commit();
            return result;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
