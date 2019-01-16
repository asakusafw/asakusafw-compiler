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
package com.asakusafw.bridge.launch;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Test for {@link StageClientOption}.
 */
public class StageClientOptionTest {

    /**
     * commands.
     * @throws Exception if failed
     */
    @Test
    public void commands() throws Exception {
        StageClientOption option = new StageClientOption(getClass().getClassLoader());
        assertThat(option.getCommands(), containsInAnyOrder("--client"));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        StageClientOption option = new StageClientOption(getClass().getClassLoader());
        option.accept("--client", getClass().getName());

        Class<?> resolved = option.resolve();
        assertThat(resolved, is((Object) getClass()));
    }

    /**
     * w/ extra value.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void extra_value() throws Exception {
        StageClientOption option = new StageClientOption(getClass().getClassLoader());
        option.accept("--client", getClass().getName());
        option.accept("--client", getClass().getName());
    }

    /**
     * w/ invalid value.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void invalid_value() throws Exception {
        StageClientOption option = new StageClientOption(getClass().getClassLoader());
        option.accept("--client", "<MISSING>");
    }

    /**
     * w/o values.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void not_set() throws Exception {
        StageClientOption option = new StageClientOption(getClass().getClassLoader());
        option.resolve();
    }
}
