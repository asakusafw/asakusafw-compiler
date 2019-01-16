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
package com.asakusafw.dag.runtime.jdbc.operation;

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
import com.asakusafw.dag.api.processor.testing.MockTaskProcessorContext;
import com.asakusafw.dag.api.processor.testing.MockVertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.ExtractOperation.Input;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.InputHandler.InputSession;
import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;
import com.asakusafw.dag.runtime.jdbc.basic.BasicJdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.testing.KsvJdbcAdapter;
import com.asakusafw.dag.runtime.jdbc.testing.KsvModel;

/**
 * Test for {@link JdbcInputAdapter}.
 */
public class JdbcInputAdapterTest extends JdbcDagTestRoot {

    private static final String PROFILE = "testing";

    private static final StageInfo STAGE = new StageInfo("u", "b", "f", "s", "e", Collections.emptyMap());

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        insert(0, null, "Hello, world!");
        JdbcEnvironment environment = environment(PROFILE);
        MockVertexProcessorContext vc = new MockVertexProcessorContext()
                .withResource(StageInfo.class, STAGE)
                .withResource(JdbcEnvironment.class, environment);
        try (JdbcInputAdapter adapter = new JdbcInputAdapter(vc)) {
            adapter.input("t", PROFILE, driver());
            adapter.initialize();
            assertThat(collect(adapter), contains(new KsvModel(0, null, "Hello, world!")));
        }
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
        JdbcEnvironment environment = environment(PROFILE);
        MockVertexProcessorContext vc = new MockVertexProcessorContext()
                .withResource(StageInfo.class, STAGE)
                .withResource(JdbcEnvironment.class, environment);
        try (JdbcInputAdapter adapter = new JdbcInputAdapter(vc)) {
            adapter.input("t", PROFILE, driver());
            adapter.initialize();
            assertThat(collect(adapter), contains(
                    new KsvModel(1, null, "Hello1"),
                    new KsvModel(2, null, "Hello2"),
                    new KsvModel(3, null, "Hello3")));
        }
    }

    /**
     * multiple tasks.
     * @throws Exception if failed
     */
    @Test
    public void multiple_tasks_separate() throws Exception {
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        insert(4, null, "Hello4");
        insert(5, null, "Hello5");
        JdbcEnvironment environment = environment(PROFILE);
        MockVertexProcessorContext vc = new MockVertexProcessorContext()
                .withResource(StageInfo.class, STAGE)
                .withResource(JdbcEnvironment.class, environment);
        try (JdbcInputAdapter adapter = new JdbcInputAdapter(vc)) {
            adapter.input("t1", PROFILE, driver("M_KEY = 1"));
            adapter.input("t2", PROFILE, driver("M_KEY = 2"));
            adapter.input("t3", PROFILE, driver("M_KEY = 3"));
            adapter.input("t4", PROFILE, driver("M_KEY = 4"));
            adapter.input("t5", PROFILE, driver("M_KEY = 5"));
            adapter.initialize();
            assertThat(collect(adapter), contains(
                    new KsvModel(1, null, "Hello1"),
                    new KsvModel(2, null, "Hello2"),
                    new KsvModel(3, null, "Hello3"),
                    new KsvModel(4, null, "Hello4"),
                    new KsvModel(5, null, "Hello5")));
        }
    }

    /**
     * multiple tasks.
     * @throws Exception if failed
     */
    @Test
    public void multiple_tasks_share() throws Exception {
        edit(it -> it.withMaxInputConcurrency(2));
        insert(1, null, "Hello1");
        insert(2, null, "Hello2");
        insert(3, null, "Hello3");
        insert(4, null, "Hello4");
        insert(5, null, "Hello5");
        JdbcEnvironment environment = environment(PROFILE);
        MockVertexProcessorContext vc = new MockVertexProcessorContext()
                .withResource(StageInfo.class, STAGE)
                .withResource(JdbcEnvironment.class, environment);
        try (JdbcInputAdapter adapter = new JdbcInputAdapter(vc)) {
            adapter.input("t1", PROFILE, driver("M_KEY = 1"));
            adapter.input("t2", PROFILE, driver("M_KEY = 2"));
            adapter.input("t3", PROFILE, driver("M_KEY = 3"));
            adapter.input("t4", PROFILE, driver("M_KEY = 4"));
            adapter.input("t5", PROFILE, driver("M_KEY = 5"));
            adapter.initialize();
            assertThat(collect(adapter), contains(
                    new KsvModel(1, null, "Hello1"),
                    new KsvModel(2, null, "Hello2"),
                    new KsvModel(3, null, "Hello3"),
                    new KsvModel(4, null, "Hello4"),
                    new KsvModel(5, null, "Hello5")));
        }
    }

    private static BasicJdbcInputDriver driver() {
        return new BasicJdbcInputDriver(SELECT, KsvJdbcAdapter::new);
    }

    private static BasicJdbcInputDriver driver(String condition) {
        return new BasicJdbcInputDriver(String.format("%s WHERE %s", SELECT, condition), KsvJdbcAdapter::new);
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
        return results;
    }
}
