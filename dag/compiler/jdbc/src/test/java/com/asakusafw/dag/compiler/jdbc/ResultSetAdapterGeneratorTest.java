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
package com.asakusafw.dag.compiler.jdbc;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

import com.asakusafw.dag.compiler.jdbc.ResultSetAdapterGenerator.Spec;
import com.asakusafw.dag.compiler.jdbc.testing.AllType;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.runtime.value.BooleanOption;
import com.asakusafw.runtime.value.ByteOption;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.DoubleOption;
import com.asakusafw.runtime.value.FloatOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.ShortOption;
import com.asakusafw.runtime.value.StringOption;

/**
 * Test for {@link ResultSetAdapterGenerator}.
 */
@SuppressWarnings("deprecation")
public class ResultSetAdapterGeneratorTest extends JdbcDagCompilerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("int")));

        h2.execute("CREATE TABLE TESTING(A INT);");
        h2.execute("INSERT INTO TESTING(A) VALUES(100)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT * FROM TESTING", rs -> {
                assertThat(rs.next(), is(true));
                AllType object = adapter.extract(rs);
                assertThat(object.getIntOption(), is(new IntOption(100)));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ multiple records.
     */
    @Test
    public void records() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("int")));

        h2.execute("CREATE TABLE TESTING(K INT, V INT);");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, 100)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, 200)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, 300)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getIntOption(), is(new IntOption(100)));
                assertThat(o0.getStringOption(), is(new StringOption()));
                o0.getStringOption().modify("BROKEN");

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getIntOption(), is(new IntOption(200)));
                assertThat(o1.getStringOption(), is(new StringOption()));
                o1.getStringOption().modify("BROKEN");

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getIntOption(), is(new IntOption(300)));
                assertThat(o2.getStringOption(), is(new StringOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ multiple columns.
     */
    @Test
    public void columns() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("int", "string", "decimal")));

        h2.execute("CREATE TABLE TESTING(A INT, B VARCHAR(256), C DECIMAL(18, 2));");
        h2.execute("INSERT INTO TESTING(A, B, C) VALUES(100, 'Hello, world!', 3.14)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT * FROM TESTING", rs -> {
                assertThat(rs.next(), is(true));
                AllType object = adapter.extract(rs);
                assertThat(object.getIntOption(), is(new IntOption(100)));
                assertThat(object.getStringOption(), is(new StringOption("Hello, world!")));
                assertThat(object.getDecimalOption(), is(new DecimalOption(new BigDecimal("3.14"))));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ boolean value.
     */
    @Test
    public void type_boolean() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("boolean")));

        h2.execute("CREATE TABLE TESTING(K INT, V BOOLEAN);");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, TRUE)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, FALSE)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getBooleanOption(), is(new BooleanOption(true)));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getBooleanOption(), is(new BooleanOption(false)));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getBooleanOption(), is(new BooleanOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ byte value.
     */
    @Test
    public void type_byte() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("byte")));

        h2.execute("CREATE TABLE TESTING(K INT, V TINYINT);");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, 0)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, 127)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getByteOption(), is(new ByteOption((byte) 0)));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getByteOption(), is(new ByteOption((byte) 127)));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getByteOption(), is(new ByteOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ short value.
     */
    @Test
    public void type_short() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("short")));

        h2.execute("CREATE TABLE TESTING(K INT, V SMALLINT);");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, 0)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, 10000)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getShortOption(), is(new ShortOption((short) 0)));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getShortOption(), is(new ShortOption((short) 10000)));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getShortOption(), is(new ShortOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ int value.
     */
    @Test
    public void type_int() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("int")));

        h2.execute("CREATE TABLE TESTING(K INT, V INT);");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, 0)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, 16777216)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getIntOption(), is(new IntOption(0)));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getIntOption(), is(new IntOption(16777216)));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getIntOption(), is(new IntOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ long value.
     */
    @Test
    public void type_long() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("long")));

        h2.execute("CREATE TABLE TESTING(K INT, V BIGINT);");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, 0)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, 123456789012345)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getLongOption(), is(new LongOption(0)));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getLongOption(), is(new LongOption(123456789012345L)));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getLongOption(), is(new LongOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ float value.
     */
    @Test
    public void type_float() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("float")));

        h2.execute("CREATE TABLE TESTING(K INT, V FLOAT);");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, 0.0)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, 1.5)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getFloatOption(), is(new FloatOption(0.0f)));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getFloatOption(), is(new FloatOption(1.5f)));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getFloatOption(), is(new FloatOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ double value.
     */
    @Test
    public void type_double() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("double")));

        h2.execute("CREATE TABLE TESTING(K INT, V DOUBLE);");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, 0.0)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, 1.5)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getDoubleOption(), is(new DoubleOption(0.0)));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getDoubleOption(), is(new DoubleOption(1.5)));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getDoubleOption(), is(new DoubleOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ decimal value.
     */
    @Test
    public void type_decimal() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("decimal")));

        h2.execute("CREATE TABLE TESTING(K INT, V DECIMAL(18, 2));");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, 0.0)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, 3.14)");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getDecimalOption(), is(new DecimalOption(new BigDecimal("0.00"))));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getDecimalOption(), is(new DecimalOption(new BigDecimal("3.14"))));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getDecimalOption(), is(new DecimalOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ string value.
     */
    @Test
    public void type_string() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("string")));

        h2.execute("CREATE TABLE TESTING(K INT, V VARCHAR(256));");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, 'Hello, world!')");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, '')");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getStringOption(), is(new StringOption("Hello, world!")));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getStringOption(), is(new StringOption("")));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getStringOption(), is(new StringOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ date value.
     */
    @Test
    public void type_date() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("date")));

        h2.execute("CREATE TABLE TESTING(K INT, V DATE);");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, '1970-01-01')");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, '2016-08-31')");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getDateOption(), is(new DateOption(new Date(1970, 1, 1))));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getDateOption(), is(new DateOption(new Date(2016, 8, 31))));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getDateOption(), is(new DateOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * w/ date-time value.
     */
    @Test
    public void type_datetime() {
        ClassData data = ResultSetAdapterGenerator.generate(
                context(),
                new Spec(typeOf(AllType.class), names("date_time")));

        h2.execute("CREATE TABLE TESTING(K INT, V TIMESTAMP);");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(0, '1970-01-01 00:00:00')");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(1, '2016-08-31 12:34:56')");
        h2.execute("INSERT INTO TESTING(K, V) VALUES(2, NULL)");
        add(data, c -> {
            @SuppressWarnings("unchecked")
            ResultSetAdapter<AllType> adapter = (ResultSetAdapter<AllType>) c.newInstance();
            run("SELECT V FROM TESTING ORDER BY K", rs -> {
                assertThat(rs.next(), is(true));
                AllType o0 = adapter.extract(rs);
                assertThat(o0.getDateTimeOption(), is(new DateTimeOption(new DateTime(1970, 1, 1, 0, 0, 0))));

                assertThat(rs.next(), is(true));
                AllType o1 = adapter.extract(rs);
                assertThat(o1.getDateTimeOption(), is(new DateTimeOption(new DateTime(2016, 8, 31, 12, 34, 56))));

                assertThat(rs.next(), is(true));
                AllType o2 = adapter.extract(rs);
                assertThat(o2.getDateTimeOption(), is(new DateTimeOption()));

                assertThat(rs.next(), is(false));
            });
        });
    }

    /**
     * cache.
     */
    @Test
    public void cache() {
        ClassData a = ResultSetAdapterGenerator.generate(context(),
                new Spec(typeOf(AllType.class), names("int")));
        ClassData b = ResultSetAdapterGenerator.generate(context(),
                new Spec(typeOf(AllType.class), names("int")));
        ClassData c = ResultSetAdapterGenerator.generate(context(),
                new Spec(typeOf(AllType.class), names("int", "double")));
        assertThat(b, is(cacheOf(a)));
        assertThat(c, is(not(cacheOf(a))));
    }

    private void run(String sql, Action<ResultSet, Exception> action) {
        try (Connection connection = h2.open();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            action.perform(resultSet);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
