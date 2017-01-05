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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;
import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.operation.OutputClearKind;
import com.asakusafw.dag.runtime.jdbc.testing.KsvJdbcAdapter;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;
import com.asakusafw.lang.utils.common.Lang;

/**
 * Test for {@link WindGateJdbcDirect}.
 */
public class WindGateJdbcDirectTest extends JdbcDagTestRoot {

    /**
     * input - simple case.
     * @throws Exception if failed
     */
    @Test
    public void input() throws Exception {
        insert(0, null, "Hello, world!");
        context("testing", c -> {
            JdbcInputDriver driver = WindGateJdbcDirect.input("testing", TABLE, COLUMNS, KsvJdbcAdapter::new)
                    .build(c);
            List<KsvModel> results = get(driver);
            assertThat(results, contains(new KsvModel(0, null, "Hello, world!")));
        });
    }

    /**
     * input - multiple records.
     * @throws Exception if failed
     */
    @Test
    public void input_multiple() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        context("testing", c -> {
            JdbcInputDriver driver = WindGateJdbcDirect.input("testing", TABLE, COLUMNS, KsvJdbcAdapter::new)
                    .build(c);
            List<KsvModel> results = get(driver);
            assertThat(results, contains(
                    new KsvModel(1, null, "Hello1"),
                    new KsvModel(2, null, "Hello2"),
                    new KsvModel(3, null, "Hello3")));
        });
    }

    /**
     * input - w/ condition.
     * @throws Exception if failed
     */
    @Test
    public void input_conditional() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        context("testing", Collections.singletonMap("V", "2"), c -> {
            JdbcInputDriver driver = WindGateJdbcDirect.input("testing", TABLE, COLUMNS, KsvJdbcAdapter::new)
                    .withCondition("M_KEY = ${V}")
                    .build(c);
            List<KsvModel> results = get(driver);
            assertThat(results, contains(new KsvModel(2, null, "Hello2")));
        });
    }

    /**
     * input - w/ split.
     * @throws Exception if failed
     */
    @Test
    public void input_split() throws Exception {
        List<KsvModel> in = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            in.add(insert(i, null, null));
        }
        edit(b -> b.withMaxInputConcurrency(10));
        context("testing", c -> {
            JdbcInputDriver driver = WindGateJdbcDirect.input("testing", TABLE, COLUMNS, KsvJdbcAdapter::new)
                    .withOption(WindGateJdbcDirect.OPTIMIAZATION_CORE_SPLIT_PREFIX + "M_KEY")
                    .build(c);
            List<List<KsvModel>> parts = connect(driver::getPartitions).stream()
                    .map(p -> Lang.safe(() -> get(p)))
                    .collect(Collectors.toList());
            assertThat(parts, hasSize(10));
            parts.forEach(e -> assertThat(e, hasSize(lessThan(20))));
            assertThat(parts.stream()
                    .flatMap(Collection::stream)
                    .sorted((a, b) -> Long.compare(a.getKey(), b.getKey()))
                    .collect(Collectors.toList()), is(in));
        });
    }

    /**
     * input - w/ split but is suppressed by configuration.
     * @throws Exception if failed
     */
    @Test
    public void input_split_suppressed() throws Exception {
        List<KsvModel> in = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            in.add(insert(i, null, null));
        }
        edit(b -> b.withMaxInputConcurrency(1));
        context("testing", c -> {
            JdbcInputDriver driver = WindGateJdbcDirect.input("testing", TABLE, COLUMNS, KsvJdbcAdapter::new)
                    .withOption(WindGateJdbcDirect.OPTIMIAZATION_CORE_SPLIT_PREFIX + "M_KEY")
                    .build(c);
            List<List<KsvModel>> parts = connect(driver::getPartitions).stream()
                    .map(p -> Lang.safe(() -> get(p)))
                    .collect(Collectors.toList());
            assertThat(parts, hasSize(1));
            assertThat(parts.stream()
                    .flatMap(Collection::stream)
                    .sorted((a, b) -> Long.compare(a.getKey(), b.getKey()))
                    .collect(Collectors.toList()), is(in));
        });
    }

    /**
     * output - simple case.
     * @throws Exception if failed
     */
    @Test
    public void output() throws Exception {
        context("testing", c -> {
            JdbcOutputDriver driver = WindGateJdbcDirect.output("testing", TABLE, COLUMNS, KsvJdbcAdapter::new)
                    .build(c);
            put(driver, new KsvModel(0, null, "Hello, world!"));
        });
        assertThat(select(), contains(new KsvModel(0, null, "Hello, world!")));
    }

    /**
     * output - multiple records.
     * @throws Exception if failed
     */
    @Test
    public void output_multiple() throws Exception {
        context("testing", c -> {
            JdbcOutputDriver driver = WindGateJdbcDirect.output("testing", TABLE, COLUMNS, KsvJdbcAdapter::new)
                    .build(c);
            put(driver,
                    new KsvModel(1, null, "Hello1"),
                    new KsvModel(2, null, "Hello2"),
                    new KsvModel(3, null, "Hello3"));
        });
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(2, null, "Hello2"),
                new KsvModel(3, null, "Hello3")));
    }

    /**
     * output - w/ oracle dirpath.
     * @throws Exception if failed
     */
    @Test
    public void output_oracle_dirpath() throws Exception {
        edit(b -> b.withOption(WindGateJdbcDirect.OPTIMIAZATION_ORACLE_DIRPATH));
        context("testing", c -> {
            JdbcOutputDriver driver = WindGateJdbcDirect.output("testing", TABLE, COLUMNS, KsvJdbcAdapter::new)
                    .withOptions(WindGateJdbcDirect.OPTIMIAZATION_ORACLE_DIRPATH)
                    .build(c);
            put(driver, new KsvModel(0, null, "Hello, world!"));
        });
        assertThat(select(), contains(new KsvModel(0, null, "Hello, world!")));
    }

    /**
     * truncate - simple.
     * @throws Exception if failed
     */
    @Test
    public void truncate() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        context("testing", c -> {
            JdbcOperationDriver driver = WindGateJdbcDirect.truncate("testing", TABLE, COLUMNS)
                    .build(c);
            perform(driver);
        });
        assertThat(select(), hasSize(0));
    }

    /**
     * truncate - simple.
     * @throws Exception if failed
     */
    @Test
    public void truncate_delete() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        edit(b -> b.withOption(OutputClearKind.DELETE));
        context("testing", c -> {
            JdbcOperationDriver driver = WindGateJdbcDirect.truncate("testing", TABLE, COLUMNS)
                    .build(c);
            perform(driver);
        });
        assertThat(select(), hasSize(0));
    }

    /**
     * truncate - simple.
     * @throws Exception if failed
     */
    @Test
    public void truncate_keep() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        edit(b -> b.withOption(OutputClearKind.KEEP));
        context("testing", c -> {
            JdbcOperationDriver driver = WindGateJdbcDirect.truncate("testing", TABLE, COLUMNS)
                    .build(c);
            perform(driver);
        });
        assertThat(select(), hasSize(3));
    }

    /**
     * output - custom truncate.
     * @throws Exception if failed
     */
    @Test
    public void truncate_custom() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        context("testing", Collections.singletonMap("V", "2"), c -> {
            JdbcOperationDriver driver = WindGateJdbcDirect.truncate("testing", TABLE, COLUMNS)
                    .withCustomTruncate(JdbcUtil.getDeleteStatement(TABLE, "M_KEY = ${V}"))
                    .build(c);
            perform(driver);
        });
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(3, null, "Hello3")));
    }
}
