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
package com.asakusafw.vanilla.testkit.inprocess;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.asakusafw.runtime.configuration.FrameworkDeployer;
import com.asakusafw.runtime.util.VariableTable;
import com.asakusafw.testdriver.TestDriverContext;
import com.asakusafw.testdriver.TestExecutionPlan;
import com.asakusafw.testdriver.hadoop.ConfigurationFactory;
import com.asakusafw.vanilla.compiler.common.VanillaTask;
import com.asakusafw.vanilla.testkit.inprocess.testing.Callback;

/**
 * Test for {@link VanillaCommandEmulator}.
 */
public class VanillaCommandEmulatorTest {

    /**
     * The framework configurator.
     */
    @Rule
    public final FrameworkDeployer framework = new FrameworkDeployer(false) {
        @Override
        protected void deploy() throws Throwable {
            copy(new File("src/test/dist"), getHome());
            context =  new TestDriverContext(VanillaCommandEmulatorTest.class) {
                @Override
                public String getDevelopmentEnvironmentVersion() {
                    return "0.0.0.testing";
                }
            };
            context.useSystemBatchApplicationsInstallationPath(true);
            context.setFrameworkHomePath(getHome());
            context.setBatchApplicationsInstallationPath(
                    new File(getHome(), TestDriverContext.DEFAULT_BATCHAPPS_PATH));
            context.setCurrentBatchId("b");
            context.setCurrentFlowId("f");
            context.setCurrentExecutionId("e");
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
            configurations = ConfigurationFactory.getDefault();
        }
        @Override
        protected void after() {
            Callback.action = null;
            if (context != null) {
                context.cleanUpTemporaryResources();
            }
        }
    };

    TestDriverContext context;

    ConfigurationFactory configurations;

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        AtomicBoolean performed = new AtomicBoolean();
        Callback.action = c -> assertThat(performed.getAndSet(true), is(false));

        VanillaCommandEmulator emulator = new VanillaCommandEmulator();
        TestExecutionPlan.Command command = command(Callback.class);
        assertThat(emulator.accepts(context, configurations, command), is(true));
        emulator.execute(context, configurations, command);

        assertThat(performed.get(), is(true));
    }

    /**
     * w/ engine conf.
     * @throws Exception if failed
     */
    @Test
    public void engine_conf() throws Exception {
        Callback.action = c -> assertThat(c.getProperty("com.asakusafw.vanilla.testing").orElse("false"), is(is("true")));

        VanillaCommandEmulator emulator = new VanillaCommandEmulator();
        TestExecutionPlan.Command command = command(Callback.class);
        assertThat(emulator.accepts(context, configurations, command), is(true));
        emulator.execute(context, configurations, command);
    }

    private TestExecutionPlan.Command command(Class<?> application) {
        List<String> cmd = new ArrayList<>();
        cmd.add(new File(framework.getHome(), VanillaTask.PATH_COMMAND.toPath()).getPath());
        cmd.add(context.getCurrentBatchId());
        cmd.add(context.getCurrentFlowId());
        cmd.add(context.getCurrentExecutionId());
        VariableTable args = new VariableTable();
        args.defineVariables(context.getBatchArgs());
        cmd.add(args.toSerialString());
        cmd.add(application.getName());

        return new TestExecutionPlan.Command(
                cmd,
                VanillaTask.MODULE_NAME,
                VanillaTask.PROFILE_NAME,
                Collections.emptyMap());
    }
}
