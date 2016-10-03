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
package com.asakusafw.dag.runtime.jdbc.basic;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;
import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver.Partition;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.testing.KsvJdbcAdapter;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;
import com.asakusafw.dag.runtime.jdbc.testing.TimeJdbcAdapter;
import com.asakusafw.dag.runtime.jdbc.testing.TimeModel;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateTime;

/**
 * Test for {@link SplitJdbcInputDriver}.
 */
public class SplitJdbcInputDriverTest extends JdbcDagTestRoot {

    static final Logger LOG = LoggerFactory.getLogger(SplitJdbcInputDriverTest.class);

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        insert(0, null, "Hello, world!");
        profile("testing", p -> {
            List<? extends Partition> parts = get(p, "M_KEY", 100, null);
            assertThat(parts, hasSize(1));
            List<KsvModel> results = get(parts.get(0));
            assertThat(results, contains(new KsvModel(0, null, "Hello, world!")));
        });
    }

    /**
     * split by key.
     * @throws Exception if failed
     */
    @Test
    public void split() throws Exception {
        List<KsvModel> in = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            in.add(insert(i, null, null));
        }
        profile("testing", p -> {
            List<List<KsvModel>> parts = sort(get(p, "M_KEY", 10, null));
            assertThat(parts, hasSize(10));
            parts.forEach(e -> assertThat(e, hasSize(lessThan(20))));
            assertThat(merge(parts), is(in));
        });
    }

    /**
     * split by decimal.
     * @throws Exception if failed
     */
    @Test
    public void split_decimal() throws Exception {
        List<KsvModel> in = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            in.add(insert(new KsvModel(i, new BigDecimal(BigInteger.valueOf((i + 1) * 7 % 1000), 2), null)));
        }
        profile("testing", p -> {
            List<List<KsvModel>> parts = sort(get(p, "M_SORT", 10, null));
            assertThat(parts, hasSize(10));
            parts.forEach(e -> assertThat(e, hasSize(lessThan(20))));
            assertThat(merge(parts), is(in));
        });
    }

    /**
     * split by date.
     * @throws Exception if failed
     */
    @Test
    public void split_date() throws Exception {
        usingTime();
        int base = new Date(2016, 9, 1).getElapsedDays();
        List<TimeModel> in = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            in.add(insertTime(new TimeModel(i, new Date(base + i), null)));
        }
        profile("testing", p -> {
            List<List<TimeModel>> parts = sortTime(getTime(p, "M_DATE", 10, null));
            assertThat(parts, hasSize(10));
            parts.forEach(e -> assertThat(e, hasSize(lessThan(20))));
            assertThat(mergeTime(parts), is(in));
        });
    }

    /**
     * split by timestamp.
     * @throws Exception if failed
     */
    @Test
    public void split_timestamp() throws Exception {
        usingTime();
        long base = new DateTime(2016, 9, 1, 0, 0, 0).getElapsedSeconds();
        List<TimeModel> in = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            in.add(insertTime(new TimeModel(i, null, new DateTime(base + i))));
        }
        profile("testing", p -> {
            List<List<TimeModel>> parts = sortTime(getTime(p, "M_TIMESTAMP", 10, null));
            assertThat(parts, hasSize(10));
            parts.forEach(e -> assertThat(e, hasSize(lessThan(20))));
            assertThat(mergeTime(parts), is(in));
        });
    }

    /**
     * split w/ condition.
     * @throws Exception if failed
     */
    @Test
    public void split_decimal_cond() throws Exception {
        List<KsvModel> in = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            in.add(insert(new KsvModel(i, new BigDecimal(BigInteger.valueOf((i + 1) * 7 % 1000), 2), null)));
        }
        profile("testing", p -> {
            List<List<KsvModel>> parts = sort(get(p, "M_SORT", 10, "M_KEY >= 100 AND M_KEY < 200"));
            assertThat(parts, hasSize(10));
            parts.forEach(e -> assertThat(e, hasSize(lessThan(20))));
            assertThat(merge(parts), is(in.stream()
                    .filter(m -> m.getKey() >= 100 && m.getKey() < 200)
                    .collect(Collectors.toList())));
        });
    }

    /**
     * split w/ dense values.
     * @throws Exception if failed
     */
    @Test
    public void split_dense() throws Exception {
        List<KsvModel> in = new ArrayList<>();
        in.add(insert(new KsvModel(-1, null, null)));
        for (int i = 0; i < 1000; i++) {
            in.add(insert(new KsvModel(i, new BigDecimal(BigInteger.valueOf(i % 3), 2), null)));
        }
        profile("testing", p -> {
            List<List<KsvModel>> parts = sort(get(p, "M_SORT", 10, null));
            assertThat(parts, hasSize(lessThanOrEqualTo(4)));
            parts.forEach(e -> assertThat(e, hasSize(lessThan(400))));
            assertThat(merge(parts), is(in));
        });
    }

    /**
     * split w/ nulls.
     * @throws Exception if failed
     */
    @Test
    public void split_nulls() throws Exception {
        List<KsvModel> in = new ArrayList<>();
        in.add(insert(new KsvModel(-1, null, null)));
        for (int i = 0; i < 100; i++) {
            in.add(insert(new KsvModel(i, new BigDecimal(BigInteger.valueOf((i + 1) * 11 % 1000), 2), null)));
        }
        profile("testing", p -> {
            List<List<KsvModel>> parts = sort(get(p, "M_SORT", 10, null));
            assertThat(parts, hasSize(10));
            parts.forEach(e -> assertThat(e, hasSize(lessThan(20))));
            assertThat(merge(parts), is(in));
        });
    }

    /**
     * split empty table.
     * @throws Exception if failed
     */
    @Test
    public void split_empty() throws Exception {
        profile("testing", p -> {
            List<List<KsvModel>> parts = sort(get(p, "M_KEY", 10, null));
            assertThat(parts, hasSize(lessThanOrEqualTo(1)));
            assertThat(merge(parts), hasSize(0));
        });
    }

    /**
     * split by unsupported type.
     * @throws Exception if failed
     */
    @Test
    public void split_unsupported() throws Exception {
        List<KsvModel> in = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            in.add(insert(i, null, "Hello" + i));
        }
        profile("testing", p -> {
            List<? extends Partition> parts = get(p, "M_VALUE", 10, null);
            assertThat(parts, hasSize(1));
            List<KsvModel> results = get(parts.get(0));
            assertThat(results, is(in));
        });
    }

    private List<? extends Partition> get(JdbcProfile profile, String split, int count, String condition) {
        return connect(new SplitJdbcInputDriver(
                profile,
                TABLE, COLUMNS,
                split, count, condition,
                KsvJdbcAdapter::new)::getPartitions);
    }

    private List<List<KsvModel>> sort(List<? extends Partition> parts) throws IOException, InterruptedException {
        return sort(parts, KsvModel::new, (a, b) -> a.getKeyOption().compareTo(b.getKeyOption()));
    }

    private List<KsvModel> merge(List<List<KsvModel>> aa) {
        return merge(aa, (a, b) -> a.getKeyOption().compareTo(b.getKeyOption()));
    }

    private void usingTime() {
        h2.execute("CREATE TABLE TEMPORAL(M_KEY INTEGER NOT NULL, M_DATE DATE, M_TIMESTAMP TIMESTAMP)");
    }

    private TimeModel insertTime(TimeModel model) throws IOException {
        try (Connection c = h2.open();
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO TEMPORAL(M_KEY,M_DATE,M_TIMESTAMP) VALUES(?,?,?)")) {
            new TimeJdbcAdapter().drive(ps, model);
            ps.executeUpdate();
            c.commit();
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
        return model;
    }

    private List<? extends Partition> getTime(JdbcProfile profile, String split, int count, String condition) {
        return connect(new SplitJdbcInputDriver(
                profile,
                "TEMPORAL", Arrays.asList("M_KEY", "M_DATE", "M_TIMESTAMP"),
                split, count, condition,
                TimeJdbcAdapter::new)::getPartitions);
    }

    private List<List<TimeModel>> sortTime(List<? extends Partition> parts) throws IOException, InterruptedException {
        return sort(parts, TimeModel::new, (a, b) -> a.getKeyOption().compareTo(b.getKeyOption()));
    }

    private List<TimeModel> mergeTime(List<List<TimeModel>> aa) {
        return merge(aa, (a, b) -> a.getKeyOption().compareTo(b.getKeyOption()));
    }

    @SuppressWarnings("unchecked")
    private <T> List<List<T>> sort(
            List<? extends Partition> parts,
            UnaryOperator<T> copier,
            Comparator<? super T> comparator) throws IOException, InterruptedException {
        List<List<T>> results = new ArrayList<>();
        for (Partition p : parts) {
            List<T> sub = new ArrayList<>();
            try (Connection c = h2.open(); ObjectReader reader = p.open(c)) {
                while (reader.nextObject()) {
                    sub.add(copier.apply((T) reader.getObject()));
                }
            } catch (SQLException e) {
                throw JdbcUtil.wrap(e);
            }
            sub.sort(comparator);
            LOG.debug("{}: {}, sample={}", results.size(), sub.size(), sub.isEmpty() ? null : sub.get(0));
            results.add(sub);
        }
        results.sort((a, b) -> {
            if (a.isEmpty() && b.isEmpty()) {
                return 0;
            }
            if (a.isEmpty()) {
                return -1;
            }
            if (b.isEmpty()) {
                return +1;
            }
            return comparator.compare(a.get(0), b.get(0));
        });
        return results;
    }

    private <T> List<T> merge(List<List<T>> aa, Comparator<? super T> comparator) {
        return aa.stream()
                .flatMap(Collection::stream)
                .sorted(comparator)
                .collect(Collectors.toList());
    }
}
