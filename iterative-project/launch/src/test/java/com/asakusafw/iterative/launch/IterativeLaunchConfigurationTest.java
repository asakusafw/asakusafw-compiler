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
package com.asakusafw.iterative.launch;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.iterative.common.IterativeExtensions;
import com.asakusafw.iterative.common.ParameterTable;
import com.asakusafw.iterative.launch.IterativeStageInfo.Cursor;

/**
 * Test for {@link IterativeLaunchConfiguration}.
 */
public class IterativeLaunchConfigurationTest {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        File file = dump(IterativeExtensions.builder()
                .next().put("a", "A")
                .build());

        String[] arguments = {
                "--client", IterativeLaunchConfiguration.class.getName(),
                "--user-name", "u",
                "--batch-id", "b",
                "--flow-id", "f",
                "--execution-id", "e",
                "--parameter-table", "@" + file.getAbsolutePath(),
        };
        IterativeLaunchConfiguration conf = IterativeLaunchConfiguration.parse(getClass().getClassLoader(), arguments);

        assertThat(conf.getStageClient(), equalTo((Object) IterativeLaunchConfiguration.class));

        IterativeStageInfo info = conf.getStageInfo();
        assertThat(info.getOrigin().getUserName(), is("u"));
        assertThat(info.getOrigin().getBatchId(), is("b"));
        assertThat(info.getOrigin().getFlowId(), is("f"));
        assertThat(info.getOrigin().getExecutionId(), is("e"));
        assertThat(info.getOrigin().getStageId(), is(nullValue()));
        assertThat(info.getOrigin().getBatchArguments(), is(map()));

        assertThat(info.getRoundCount(), is(1));
        Cursor cursor = info.newCursor();

        assertThat(cursor.next(), is(true));
        StageInfo r0 = cursor.get();
        assertThat(r0.getUserName(), is("u"));
        assertThat(r0.getBatchId(), is("b"));
        assertThat(r0.getFlowId(), is("f"));
        assertThat(r0.getExecutionId(), is("e"));
        assertThat(r0.getStageId(), is(notNullValue()));
        assertThat(r0.getBatchArguments(), is(map("a", "A")));

        Map<String, String> engine = conf.getEngineProperties();
        assertThat(engine.keySet(), hasSize(0));

        Map<String, String> hadoop = conf.getHadoopProperties();
        assertThat(hadoop.keySet(), hasSize(0));
    }

    /**
     * w/o parameter table.
     * @throws Exception if failed
     */
    @Test
    public void no_parameter_table() throws Exception {
        String[] arguments = {
                "--client", IterativeLaunchConfiguration.class.getName(),
                "--user-name", "u",
                "--batch-id", "b",
                "--flow-id", "f",
                "--execution-id", "e",
        };
        IterativeLaunchConfiguration conf = IterativeLaunchConfiguration.parse(getClass().getClassLoader(), arguments);

        assertThat(conf.getStageClient(), equalTo((Object) IterativeLaunchConfiguration.class));

        IterativeStageInfo info = conf.getStageInfo();
        assertThat(info.getOrigin().getUserName(), is("u"));
        assertThat(info.getOrigin().getBatchId(), is("b"));
        assertThat(info.getOrigin().getFlowId(), is("f"));
        assertThat(info.getOrigin().getExecutionId(), is("e"));
        assertThat(info.getOrigin().getStageId(), is(nullValue()));
        assertThat(info.getOrigin().getBatchArguments(), is(map()));

        assertThat(info.getRoundCount(), is(1));
        Cursor cursor = info.newCursor();

        assertThat(cursor.next(), is(true));
        StageInfo r0 = cursor.get();
        assertThat(r0.getUserName(), is("u"));
        assertThat(r0.getBatchId(), is("b"));
        assertThat(r0.getFlowId(), is("f"));
        assertThat(r0.getExecutionId(), is("e"));
        assertThat(r0.getStageId(), is(notNullValue()));
        assertThat(r0.getBatchArguments(), is(map()));

        Map<String, String> engine = conf.getEngineProperties();
        assertThat(engine.keySet(), hasSize(0));

        Map<String, String> hadoop = conf.getHadoopProperties();
        assertThat(hadoop.keySet(), hasSize(0));
    }

    private File dump(ParameterTable table) throws IOException {
        File file = temporary.newFile("testing.json");
        try (OutputStream output = new FileOutputStream(file)) {
            IterativeExtensions.save(output, table);
        }
        return file;
    }

    private Map<String, String> map(String... pairs) {
        assertThat(pairs.length % 2, is(0));
        Map<String, String> results = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            results.put(pairs[i + 0], pairs[i + 1]);
        }
        return results;
    }
}
