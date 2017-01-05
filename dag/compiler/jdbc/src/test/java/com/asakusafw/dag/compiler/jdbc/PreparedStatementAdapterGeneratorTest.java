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
package com.asakusafw.dag.compiler.jdbc;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.asakusafw.dag.compiler.jdbc.PreparedStatementAdapterGenerator.Spec;
import com.asakusafw.dag.compiler.jdbc.testing.AllType;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.jdbc.PreparedStatementAdapter;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateTime;

/**
 * Test for {@link PreparedStatementAdapterGenerator}.
 */
@SuppressWarnings("deprecation")
public class PreparedStatementAdapterGeneratorTest extends JdbcDagCompilerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("int")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V INT);");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getIntOption().modify(100);
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(Integer.class), contains(100));
    }

    /**
     * multiple records.
     */
    @Test
    public void records() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("int")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V INT);");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getIntOption().modify(100);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getIntOption().modify(200);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getIntOption().modify(300);
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(Integer.class), contains(100, 200, 300));
    }

    /**
     * multiple columns.
     */
    @Test
    public void columns() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("int", "string", "decimal")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, A INT, B VARCHAR(256), C DECIMAL(18,2));");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(A, B, C) VALUES(?, ?, ?)", ps -> {
                AllType buf = new AllType();

                buf.getIntOption().modify(100);
                buf.getStringOption().modify("Hello1");
                buf.getDecimalOption().modify(BigDecimal.valueOf(1));
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getIntOption().modify(200);
                buf.getStringOption().modify("Hello2");
                buf.getDecimalOption().modify(BigDecimal.valueOf(2));
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getIntOption().modify(300);
                buf.getStringOption().modify("Hello3");
                buf.getDecimalOption().modify(BigDecimal.valueOf(3));
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(h2.query("SELECT A, B, C FROM TESTING ORDER BY K"), is(Arrays.asList(
                Arrays.asList(100, "Hello1", new BigDecimal("1.00")),
                Arrays.asList(200, "Hello2", new BigDecimal("2.00")),
                Arrays.asList(300, "Hello3", new BigDecimal("3.00")))));
    }

    /**
     * w/ boolean value.
     */
    @Test
    public void type_boolean() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("boolean")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V BOOLEAN)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getBooleanOption().modify(true);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getBooleanOption().modify(false);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getBooleanOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(Boolean.class), is(Arrays.asList(true, false, null)));
    }

    /**
     * w/ byte value.
     */
    @Test
    public void type_byte() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("byte")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V TINYINT)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getByteOption().modify((byte) 0);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getByteOption().modify((byte) 100);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getByteOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(Byte.class), is(Arrays.asList((byte) 0, (byte) 100, null)));
    }

    /**
     * w/ short value.
     */
    @Test
    public void type_short() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("short")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V SMALLINT)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getShortOption().modify((short) 0);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getShortOption().modify((short) 10000);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getShortOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(Short.class), is(Arrays.asList((short) 0, (short) 10000, null)));
    }

    /**
     * w/ int value.
     */
    @Test
    public void type_int() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("int")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V INT)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getIntOption().modify(0);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getIntOption().modify(1234567890);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getIntOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(Integer.class), is(Arrays.asList(0, 1234567890, null)));
    }

    /**
     * w/ long value.
     */
    @Test
    public void type_long() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("long")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V BIGINT)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getLongOption().modify(0L);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getLongOption().modify(123456789012345L);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getLongOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(Long.class), is(Arrays.asList(0L, 123456789012345L, null)));
    }

    /**
     * w/ float value.
     */
    @Test
    public void type_float() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("float")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V FLOAT)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getFloatOption().modify(0f);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getFloatOption().modify(1.5f);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getFloatOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        // H2 may return double values
        assertThat(
                Lang.project(collect(Number.class), f -> f == null ? null : f.floatValue()),
                is(Arrays.asList(0f, 1.5f, null)));
    }

    /**
     * w/ double value.
     */
    @Test
    public void type_double() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("double")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V DOUBLE)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getDoubleOption().modify(0.0);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getDoubleOption().modify(1.5);
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getDoubleOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(Double.class), is(Arrays.asList(0.0, 1.5, null)));
    }

    /**
     * w/ decimal value.
     */
    @Test
    public void type_decimal() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("decimal")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V DECIMAL(18,2))");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getDecimalOption().modify(new BigDecimal("0.0"));
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getDecimalOption().modify(new BigDecimal("3.14"));
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getDecimalOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(BigDecimal.class), is(Arrays.asList(new BigDecimal("0.00"), new BigDecimal("3.14"), null)));
    }

    /**
     * w/ string value.
     */
    @Test
    public void type_string() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("string")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V VARCHAR(256))");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getStringOption().modify("Hello, world!");
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getStringOption().modify("");
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getStringOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(String.class), is(Arrays.asList("Hello, world!", "", null)));
    }

    /**
     * w/ date value.
     */
    @Test
    public void type_date() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("date")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V DATE)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getDateOption().modify(new Date(1970, 1, 2));
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getDateOption().modify(new Date(2016, 8, 31));
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getDateOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(java.sql.Date.class), is(Arrays.asList(
                new java.sql.Date(1970 - 1900, 0, 2),
                new java.sql.Date(2016 - 1900, 7, 31),
                null)));
    }

    /**
     * w/ date-time value.
     */
    @Test
    public void type_datetime() {
        ClassData data = PreparedStatementAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("date_time")));

        h2.execute("CREATE TABLE TESTING(K INT AUTO_INCREMENT, V TIMESTAMP)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            PreparedStatementAdapter<AllType> adapter = (PreparedStatementAdapter<AllType>) c.newInstance();
            run("INSERT INTO TESTING(V) VALUES(?)", ps -> {
                AllType buf = new AllType();

                buf.getDateTimeOption().modify(new DateTime(1970, 1, 2, 0, 0, 0));
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getDateTimeOption().modify(new DateTime(2016, 8, 31, 12, 34, 56));
                adapter.drive(ps, buf);
                ps.addBatch();

                buf.getDateTimeOption().setNull();
                adapter.drive(ps, buf);
                ps.addBatch();
            });
        });
        assertThat(collect(java.sql.Timestamp.class), is(Arrays.asList(
                new java.sql.Timestamp(1970 - 1900, 0, 2, 0, 0, 0, 0),
                new java.sql.Timestamp(2016 - 1900, 7, 31, 12, 34, 56, 0),
                null)));
    }

    /**
     * cache.
     */
    @Test
    public void cache() {
        ClassData a = PreparedStatementAdapterGenerator.generate(context(),
                new Spec(typeOf(AllType.class), names("int")));
        ClassData b = PreparedStatementAdapterGenerator.generate(context(),
                new Spec(typeOf(AllType.class), names("int")));
        ClassData c = PreparedStatementAdapterGenerator.generate(context(),
                new Spec(typeOf(AllType.class), names("int", "double")));
        assertThat(b, is(cacheOf(a)));
        assertThat(c, is(not(cacheOf(a))));
    }

    private void run(String sql, Action<PreparedStatement, Exception> action) {
        try (Connection connection = h2.open();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            action.perform(statement);
            statement.executeBatch();
            connection.commit();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private <T> List<T> collect(Class<T> type) {
        return h2.query("SELECT V FROM TESTING ORDER BY K").stream()
                .map(l -> l.get(0))
                .map(type::cast)
                .collect(Collectors.toList());
    }
}
