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
package com.asakusafw.dag.compiler.jdbc.windgate;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.testing.VertexProcessorRunner;
import com.asakusafw.dag.compiler.jdbc.JdbcDagCompilerTestRoot;
import com.asakusafw.dag.compiler.jdbc.windgate.WindGateJdbcOutputProcessorGenerator.Spec;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.io.UnionRecord;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcEnvironment;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcOutputProcessor;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;

/**
 * Test for {@link WindGateJdbcOutputProcessorGenerator}.
 */
public class WindGateJdbcOutputProcessorGeneratorTest extends JdbcDagCompilerTestRoot {

    private static final String PROFILE = "testing";

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        insert(999, null, "ERROR");
        ClassData data = WindGateJdbcOutputProcessorGenerator.generate(context(), Arrays.asList(
                new Spec("x", new WindGateJdbcOutputModel(typeOf(KsvModel.class), PROFILE, TABLE, MAPPINGS))));
        run(data, new Object[][] {
            {
                new KsvModel(0, null, "Hello, world!"),
            },
        });
        assertThat(select(), contains(new KsvModel(0, null, "Hello, world!")));
    }

    /**
     * w/o output.
     * @throws Exception if failed
     */
    @Test
    public void initialize_only() throws Exception {
        insert(1, null, "Hello1");
        ClassData data = WindGateJdbcOutputProcessorGenerator.generate(context(), Arrays.asList(
                new Spec("x", new WindGateJdbcOutputModel(typeOf(KsvModel.class), PROFILE, TABLE, MAPPINGS))
                    .withOutput(false)));
        run(data, new Object[0][]);
        assertThat(select(), hasSize(0));
    }

    /**
     * multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple_records() throws Exception {
        insert(999, null, "ERROR");
        ClassData data = WindGateJdbcOutputProcessorGenerator.generate(context(), Arrays.asList(
                new Spec("x", new WindGateJdbcOutputModel(typeOf(KsvModel.class), PROFILE, TABLE, MAPPINGS))));
        run(data, new Object[][] {
            {
                new KsvModel(1, null, "Hello1"),
                new KsvModel(2, null, "Hello2"),
                new KsvModel(3, null, "Hello3")
            },
        });
        assertThat(select(), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(2, null, "Hello2"),
                new KsvModel(3, null, "Hello3")));
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
        ClassData data = WindGateJdbcOutputProcessorGenerator.generate(context(), Arrays.asList(
                new Spec("a", new WindGateJdbcOutputModel(typeOf(KsvModel.class), PROFILE, "T0", MAPPINGS)),
                new Spec("b", new WindGateJdbcOutputModel(typeOf(KsvModel.class), PROFILE, "T1", MAPPINGS)),
                new Spec("c", new WindGateJdbcOutputModel(typeOf(KsvModel.class), PROFILE, "T2", MAPPINGS))));
        run(data, new Object[][] {
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
        try (Connection conn = h2.open()) {
            assertThat(select(conn, "T0"), contains(new KsvModel(0, null, "Hello, T0!")));
            assertThat(select(conn, "T1"), contains(new KsvModel(1, null, "Hello, T1!")));
            assertThat(select(conn, "T2"), contains(new KsvModel(2, null, "Hello, T2!")));
        }
    }

    /**
     * w/ custom truncate.
     * @throws Exception if failed
     */
    @Test
    public void truncate() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        ClassData data = WindGateJdbcOutputProcessorGenerator.generate(context(), Arrays.asList(
                new Spec("x", new WindGateJdbcOutputModel(typeOf(KsvModel.class), PROFILE, TABLE, MAPPINGS)
                        .withCustomTruncate("DELETE KSV WHERE M_KEY != 2"))));
        run(data, new Object[][] {
            {
                new KsvModel(0, null, "Hello, world!"),
            },
        });
        assertThat(select(), contains(
                new KsvModel(0, null, "Hello, world!"),
                new KsvModel(2, null, "Hello2")));
    }

    /**
     * w/ options.
     * @throws Exception if failed
     */
    @Test
    public void options() throws Exception {
        insert(999, null, "ERROR");
        ClassData data = WindGateJdbcOutputProcessorGenerator.generate(context(), Arrays.asList(
                new Spec("x", new WindGateJdbcOutputModel(typeOf(KsvModel.class), PROFILE, TABLE, MAPPINGS)
                        .withOptions("O", "P", "T"))));
        run(data, new Object[][] {
            {
                new KsvModel(0, null, "Hello, world!"),
            },
        });
        assertThat(select(), contains(new KsvModel(0, null, "Hello, world!")));
    }

    private void run(ClassData data, Object[][] values) {
        List<UnionRecord> records = new ArrayList<>();
        for (int index = 0; index < values.length; index++) {
            Object[] inputs = values[index];
            for (int j = 0; j < inputs.length; j++) {
                records.add(new UnionRecord(index, inputs[j]));
            }
        }
        Collections.shuffle(records, new Random(6502));
        add(data, c -> {
            VertexProcessorRunner runner = new VertexProcessorRunner(() -> (VertexProcessor) c.newInstance());
            if (values.length > 0) {
                runner.input(JdbcOutputProcessor.INPUT_NAME, records.toArray());
            }
            runner
                .resource(StageInfo.class, STAGE)
                .resource(JdbcEnvironment.class, environment(PROFILE))
                .run();
        });
    }
}
