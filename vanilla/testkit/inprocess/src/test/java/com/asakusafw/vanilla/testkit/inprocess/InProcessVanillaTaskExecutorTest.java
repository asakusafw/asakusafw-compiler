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
package com.asakusafw.vanilla.testkit.inprocess;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.asakusafw.runtime.configuration.FrameworkDeployer;
import com.asakusafw.vanilla.compiler.common.VanillaTask;
import com.asakusafw.vanilla.testkit.common.VanillaTaskInfo;
import com.asakusafw.vanilla.testkit.common.VanillaTaskInfo.Requiremnt;
import com.asakusafw.vanilla.testkit.inprocess.testing.Callback;
import com.asakusafw.workflow.executor.TaskExecutionContext;
import com.asakusafw.workflow.executor.TaskExecutor;
import com.asakusafw.workflow.executor.TaskExecutors;
import com.asakusafw.workflow.model.CommandToken;
import com.asakusafw.workflow.model.TaskInfo;
import com.asakusafw.workflow.model.basic.BasicCommandTaskInfo;

/**
 * Test for {@link InProcessVanillaTaskExecutor}.
 */
public class InProcessVanillaTaskExecutorTest {

    /**
     * The framework configurator.
     */
    @Rule
    public final FrameworkDeployer framework = new FrameworkDeployer(false) {
        @Override
        protected void deploy() throws Throwable {
            copy(new File("src/test/dist"), getHome());

            Map<String, String> env = new LinkedHashMap<>(System.getenv());
            env.put(TaskExecutors.ENV_FRAMEWORK_PATH, getHome().getAbsolutePath());
            env.remove(TaskExecutors.ENV_BATCHAPPS_PATH);

            context = new Mock();
            context.env.putAll(env);
        }
    };

    /**
     * Disposes objects.
     */
    @Rule
    public final ExternalResource disposer = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            Callback.action = null;
        }
        @Override
        protected void after() {
            Callback.action = null;
        }
    };

    Mock context;

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        AtomicBoolean performed = new AtomicBoolean();
        Callback.action = c -> assertThat(performed.getAndSet(true), is(false));

        TaskExecutor executor = new InProcessVanillaTaskExecutor();
        TaskInfo task = command(Callback.class);
        assertThat(executor.isSupported(context, task), is(true));
        executor.execute(context, task);

        assertThat(performed.get(), is(true));
    }

    /**
     * w/ engine conf.
     * @throws Exception if failed
     */
    @Test
    public void engine_conf() throws Exception {
        Callback.action = c -> assertThat(c.getProperty("com.asakusafw.vanilla.testing").orElse("false"), is(is("true")));

        TaskExecutor executor = new InProcessVanillaTaskExecutor();
        TaskInfo task = command(Callback.class, Requiremnt.ENGINE_CONFIGURATION_FILE);
        assertThat(executor.isSupported(context, task), is(true));
        executor.execute(context, task);
    }

    /**
     * w/ requirements.
     * @throws Exception if failed
     */
    @Test
    public void requirements() throws Exception {
        TaskExecutor executor = new InProcessVanillaTaskExecutor();
        assertThat(executor.isSupported(context, command(Callback.class)), is(true));
        assertThat(executor.isSupported(context, command(Callback.class, Requiremnt.CORE_CONFIGURATION_FILE)), is(false));
        assertThat(executor.isSupported(context, command(Callback.class, Requiremnt.ENGINE_CONFIGURATION_FILE)), is(true));
        assertThat(executor.isSupported(context, command(Callback.class, (Requiremnt[]) null)), is(false));
    }

    private static BasicCommandTaskInfo command(Class<?> application, Requiremnt... requirements) {
        List<CommandToken> commandArgs = new ArrayList<>();
        commandArgs.add(CommandToken.BATCH_ID);
        commandArgs.add(CommandToken.FLOW_ID);
        commandArgs.add(CommandToken.EXECUTION_ID);
        commandArgs.add(CommandToken.BATCH_ARGUMENTS);
        commandArgs.add(CommandToken.of(application.getName()));
        BasicCommandTaskInfo task = new BasicCommandTaskInfo(
                VanillaTask.MODULE_NAME,
                VanillaTask.PROFILE_NAME,
                VanillaTask.PATH_COMMAND.toPath(),
                commandArgs);
        if (requirements != null) {
            task.addAttribute(new VanillaTaskInfo(Arrays.asList(requirements)));
        }
        return task;
    }

    private static class Mock implements TaskExecutionContext {

        final Map<String, String> conf = new LinkedHashMap<>();

        final Map<String, String> env = new LinkedHashMap<>();

        final Map<String, String> args = new LinkedHashMap<>();

        Mock() {
            return;
        }

        @Override
        public ClassLoader getClassLoader() {
            return getClass().getClassLoader();
        }

        @Override
        public Map<String, String> getConfigurations() {
            return conf;
        }

        @Override
        public Map<String, String> getEnvironmentVariables() {
            return env;
        }

        @Override
        public Map<String, String> getBatchArguments() {
            return args;
        }

        @Override
        public <T> Optional<T> findResource(Class<T> type) {
            if (type == Configuration.class) {
                return Optional.of(type.cast(new Configuration()));
            }
            return null;
        }

        @Override
        public String getBatchId() {
            return "b";
        }

        @Override
        public String getFlowId() {
            return "f";
        }

        @Override
        public String getExecutionId() {
            return "e";
        }
    }
}
