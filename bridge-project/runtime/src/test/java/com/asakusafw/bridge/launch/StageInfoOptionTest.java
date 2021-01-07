/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.bridge.launch;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;

/**
 * Test for {@link StageInfoOption}.
 */
public class StageInfoOptionTest {

    /**
     * commands.
     * @throws Exception if failed
     */
    @Test
    public void commands() throws Exception {
        StageInfoOption option = new StageInfoOption();
        assertThat(option.getCommands(), containsInAnyOrder(new String[] {
                "--user-name",
                "--batch-id",
                "--flow-id",
                "--stage-id",
                "--execution-id",
                "--batch-arguments",
        }));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        StageInfoOption option = new StageInfoOption("default");
        option.accept("--batch-id", "b");
        option.accept("--flow-id", "f");
        option.accept("--execution-id", "e");

        StageInfo result = option.resolve();
        assertThat(result.getUserName(), is("default"));
        assertThat(result.getBatchId(), is("b"));
        assertThat(result.getFlowId(), is("f"));
        assertThat(result.getStageId(), is(nullValue()));
        assertThat(result.getExecutionId(), is("e"));
        assertThat(result.getBatchArguments().keySet(), hasSize(0));
    }

    /**
     * all options.
     * @throws Exception if failed
     */
    @Test
    public void full() throws Exception {
        StageInfoOption option = new StageInfoOption("default");
        option.accept("--user-name", "u");
        option.accept("--batch-id", "b");
        option.accept("--flow-id", "f");
        option.accept("--stage-id", "s");
        option.accept("--execution-id", "e");
        option.accept("--batch-arguments", "a=A,b=B,c=C");

        StageInfo result = option.resolve();
        assertThat(result.getUserName(), is("u"));
        assertThat(result.getBatchId(), is("b"));
        assertThat(result.getFlowId(), is("f"));
        assertThat(result.getStageId(), is("s"));
        assertThat(result.getExecutionId(), is("e"));

        Map<String, String> args = result.getBatchArguments();
        assertThat(args.keySet(), hasSize(3));
        assertThat(args, hasEntry("a", "A"));
        assertThat(args, hasEntry("b", "B"));
        assertThat(args, hasEntry("c", "C"));
    }

    /**
     * conflict.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void conflict() throws Exception {
        StageInfoOption option = new StageInfoOption("default");
        option.accept("--batch-id", "b");
        option.accept("--flow-id", "f");
        option.accept("--batch-id", "b");
    }

    /**
     * w/o batch id.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void miss_batch_id() throws Exception {
        StageInfoOption option = new StageInfoOption("default");
        option.accept("--flow-id", "f");
        option.accept("--execution-id", "e");

        option.resolve();
    }

    /**
     * w/o flow id.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void miss_flow_id() throws Exception {
        StageInfoOption option = new StageInfoOption("default");
        option.accept("--batch-id", "b");
        option.accept("--execution-id", "e");

        option.resolve();
    }

    /**
     * w/o execution id.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void miss_execution_id() throws Exception {
        StageInfoOption option = new StageInfoOption("default");
        option.accept("--batch-id", "b");
        option.accept("--flow-id", "f");

        option.resolve();
    }
}
