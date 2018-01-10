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
package com.asakusafw.dag.runtime.jdbc.operation;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.testing.VertexProcessorRunner;
import com.asakusafw.dag.runtime.io.UnionRecord;
import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;
import com.asakusafw.dag.runtime.jdbc.JdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.testing.KsvJdbcAdapter;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.utils.common.Action;

/**
 * Test for {@link JdbcOutputProcessor}.
 */
public class JdbcOutputProcessorTest extends JdbcDagTestRoot {

    private static final String PROFILE = "testing";

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        insert(999, null, "ERROR");
        profile(PROFILE, profile -> {
            run(c -> c
                    .initialize("t", profile.getName(), truncate(TABLE))
                    .output("t", profile.getName(), output(TABLE)), new Object[][] {
                {
                    new KsvModel(0, null, "Hello, world!"),
                },
            });
        });
        assertThat(select(), contains(new KsvModel(0, null, "Hello, world!")));
    }

    /**
     * empty outputs.
     * @throws Exception if failed
     */
    @Test
    public void empty_records() throws Exception {
        insert(999, null, "ERROR");
        profile(PROFILE, profile -> {
            run(c -> c
                    .initialize("t", profile.getName(), truncate(TABLE))
                    .output("t", profile.getName(), output(TABLE)), new Object[0][0]);
        });
        assertThat(select(), hasSize(0));
    }

    /**
     * multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple_records() throws Exception {
        insert(999, null, "ERROR");
        profile(PROFILE, profile -> {
            run(c -> c
                    .initialize("t", profile.getName(), truncate(TABLE))
                    .output("t", profile.getName(), output(TABLE)), new Object[][] {
                {
                    new KsvModel(1, null, "Hello1"),
                    new KsvModel(2, null, "Hello2"),
                    new KsvModel(3, null, "Hello3")
                },
            });
        });
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(2, null, "Hello2"),
                new KsvModel(3, null, "Hello3")));
    }

    /**
     * may flush.
     * @throws Exception if failed
     */
    @Test
    public void flush() throws Exception {
        edit(b -> b.withInsertSize(10));
        List<KsvModel> records = new ArrayList<>();
        for (int i = 0; i < 1230; i++) {
            records.add(new KsvModel(i, null, "Hello" + i));
        }
        profile(PROFILE, profile -> {
            run(c -> c.output("t", profile.getName(), output(TABLE)), new Object[][] {
                records.toArray()
            });
        });
        assertThat(select(), is(records));
    }

    /**
     * multiple destinations.
     * @throws Exception if failed
     */
    @Test
    public void multiple_destination() throws Exception {
        h2.execute(String.format(DDL_FORMAT, "T0"));
        h2.execute(String.format(DDL_FORMAT, "T1"));
        h2.execute(String.format(DDL_FORMAT, "T2"));
        profile(PROFILE, profile -> {
            run(c -> c
                    .output("t0", profile.getName(), output("T0"))
                    .output("t1", profile.getName(), output("T1"))
                    .output("t2", profile.getName(), output("T2")), new Object[][] {
                {
                    new KsvModel(0, null, "Hello, T0!"),
                },
                {
                    new KsvModel(1, null, "Hello, T1!"),
                },
                {
                    new KsvModel(2, null, "Hello, T2!"),
                },
            });
        });
        try (Connection conn = h2.open()) {
            assertThat(select(conn, "T0"), contains(new KsvModel(0, null, "Hello, T0!")));
            assertThat(select(conn, "T1"), contains(new KsvModel(1, null, "Hello, T1!")));
            assertThat(select(conn, "T2"), contains(new KsvModel(2, null, "Hello, T2!")));
        }
    }

    private static JdbcOutputDriver output(String table) {
        return new BasicJdbcOutputDriver(
                JdbcUtil.getInsertStatement(table, COLUMNS),
                KsvJdbcAdapter::new);
    }

    private static JdbcOperationDriver truncate(String table) {
        return new BasicJdbcOperationDriver(JdbcUtil.getTruncateStatement(table));
    }

    private void run(Action<JdbcOutputProcessor, Exception> config, Object[][] values) {
        VertexProcessorRunner runner = new VertexProcessorRunner(() -> {
            JdbcOutputProcessor proc = new JdbcOutputProcessor();
            config.perform(proc);
            return proc;
        });
        List<UnionRecord> records = new ArrayList<>();
        for (int index = 0; index < values.length; index++) {
            Object[] inputs = values[index];
            for (int j = 0; j < inputs.length; j++) {
                records.add(new UnionRecord(index, inputs[j]));
            }
        }
        Collections.shuffle(records, new Random(6502));
        runner
            .input(JdbcOutputProcessor.INPUT_NAME, records.toArray())
            .resource(StageInfo.class, STAGE)
            .resource(JdbcEnvironment.class, environment(PROFILE))
            .run();
    }
}
