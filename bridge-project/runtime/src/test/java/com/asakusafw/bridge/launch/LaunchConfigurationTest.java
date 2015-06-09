/**
 * Copyright 2011-2015 Asakusa Framework Team.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;

/**
 * Test for {@link LaunchConfiguration}.
 */
public class LaunchConfigurationTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        String[] arguments = {
                "--client", LaunchConfiguration.class.getName(),
                "--user-name", "u",
                "--batch-id", "b",
                "--flow-id", "f",
                "--execution-id", "e",
        };
        LaunchConfiguration conf = LaunchConfiguration.parse(getClass().getClassLoader(), arguments);

        assertThat(conf.getStageClient(), equalTo((Object) LaunchConfiguration.class));

        StageInfo info = conf.getStageInfo();
        assertThat(info.getUserName(), is("u"));
        assertThat(info.getBatchId(), is("b"));
        assertThat(info.getFlowId(), is("f"));
        assertThat(info.getStageId(), is(nullValue()));
        assertThat(info.getExecutionId(), is("e"));
        assertThat(info.getBatchArguments().keySet(), hasSize(0));

        Map<String, String> engine = conf.getEngineProperties();
        assertThat(engine.keySet(), hasSize(0));

        Map<String, String> hadoop = conf.getHadoopProperties();
        assertThat(hadoop.keySet(), hasSize(0));
    }

    /**
     * w/ empty commands.
     * @throws Exception if failed
     */
    @Test
    public void skip_empty_commands() throws Exception {
        String[] arguments = {
                "--client", LaunchConfiguration.class.getName(),
                "",
                "--user-name", "u",
                "",
                "--batch-id", "b",
                "",
                "--flow-id", "f",
                "",
                "--execution-id", "e",
                "",
        };
        LaunchConfiguration.parse(getClass().getClassLoader(), arguments);
    }

    /**
     * w/ empty arguments.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void empty_arguments() throws Exception {
        LaunchConfiguration.parse(getClass().getClassLoader());
    }

    /**
     * w/ unknown command.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void unknown_command() throws Exception {
        String[] arguments = {
                "--UNKNOWN", "u",
        };
        LaunchConfiguration.parse(getClass().getClassLoader(), arguments);
    }

    /**
     * w/o command value.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void missing_command_value() throws Exception {
        String[] arguments = {
                "--batch-id",
        };
        LaunchConfiguration.parse(getClass().getClassLoader(), arguments);
    }

    /**
     * w/ conflict commands.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void conflict_command() throws Exception {
        List<String> arguments = Arrays.asList(new String[] {
                "--client", LaunchConfiguration.class.getName(),
                "--user-name", "u",
                "--batch-id", "b",
                "--flow-id", "f",
                "--execution-id", "e",
        });
        LaunchConfiguration.parse(getClass().getClassLoader(), arguments, new LaunchOption<Object>() {
            @Override
            public Set<String> getCommands() {
                return Collections.singleton("--client");
            }
            @Override
            public void accept(String command, String value) throws LaunchConfigurationException {
                throw new UnsupportedOperationException();
            }
            @Override
            public Object resolve() throws LaunchConfigurationException {
                throw new UnsupportedOperationException();
            }
        });
    }
}
