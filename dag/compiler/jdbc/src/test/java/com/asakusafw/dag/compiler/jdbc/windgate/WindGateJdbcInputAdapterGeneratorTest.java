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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockTaskProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.compiler.jdbc.JdbcDagCompilerTestRoot;
import com.asakusafw.dag.compiler.jdbc.windgate.WindGateJdbcInputAdapterGenerator.Spec;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.adapter.ExtractOperation.Input;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.InputHandler.InputSession;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcEnvironment;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcInputAdapter;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;

/**
 * Test for {@link WindGateJdbcInputAdapterGenerator}.
 */
public class WindGateJdbcInputAdapterGeneratorTest extends JdbcDagCompilerTestRoot {

    private static final String PROFILE = "testing";

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        insert(0, null, "Hello, world!");
        ClassData data = WindGateJdbcInputAdapterGenerator.generate(context(), new Spec("x",
                new WindGateJdbcInputModel(typeOf(KsvModel.class), PROFILE, TABLE, MAPPINGS)));
        assertThat(run(data), contains(new KsvModel(0, null, "Hello, world!")));
    }

    /**
     * multiple records.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        ClassData data = WindGateJdbcInputAdapterGenerator.generate(context(), new Spec("x",
                new WindGateJdbcInputModel(typeOf(KsvModel.class), PROFILE, TABLE, MAPPINGS)));
        assertThat(run(data), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(2, null, "Hello2"),
                new KsvModel(3, null, "Hello3")));
    }

    /**
     * w/ condition.
     * @throws Exception if failed
     */
    @Test
    public void condition() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        ClassData data = WindGateJdbcInputAdapterGenerator.generate(context(), new Spec("x",
                new WindGateJdbcInputModel(typeOf(KsvModel.class), PROFILE, TABLE, MAPPINGS)
                    .withCondition("M_KEY != 2")));
        assertThat(run(data), contains(
                new KsvModel(1, null, "Hello1"),
                new KsvModel(3, null, "Hello3")));
    }

    /**
     * w/ options.
     * @throws Exception if failed
     */
    @Test
    public void options() throws Exception {
        insert(0, null, "Hello, world!");
        ClassData data = WindGateJdbcInputAdapterGenerator.generate(context(), new Spec("x",
                new WindGateJdbcInputModel(typeOf(KsvModel.class), PROFILE, TABLE, MAPPINGS)
                       .withOptions("O", "P", "T")));
        assertThat(run(data), contains(new KsvModel(0, null, "Hello, world!")));
    }

    private List<KsvModel> run(ClassData data) {
        List<KsvModel> results = new ArrayList<>();
        add(data, c -> {
            try (JdbcInputAdapter adapter = (JdbcInputAdapter) c.getConstructor(VertexProcessorContext.class)
                    .newInstance(new MockVertexProcessorContext()
                            .withResource(StageInfo.class, STAGE)
                            .withResource(JdbcEnvironment.class, environment(PROFILE)))) {
                adapter.initialize();
                results.addAll(collect(adapter));
            }
        });
        return results;
    }

    private static List<KsvModel> collect(JdbcInputAdapter adapter) throws IOException, InterruptedException {
        List<KsvModel> results = new ArrayList<>();
        TaskSchedule schedule = adapter.getSchedule();
        InputHandler<Input, ? super TaskProcessorContext> handler = adapter.newHandler();
        for (TaskInfo info : schedule.getTasks()) {
            try (InputSession<Input> session = handler.start(new MockTaskProcessorContext(info))) {
                while (session.next()) {
                    Input input = session.get();
                    results.add(new KsvModel(input.getObject()));
                }
            }
        }
        results.sort((a, b) -> Long.compare(a.getKey(), b.getKey()));
        return results;
    }
}
