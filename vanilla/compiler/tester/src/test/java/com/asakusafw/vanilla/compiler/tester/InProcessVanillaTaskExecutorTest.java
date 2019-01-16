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
package com.asakusafw.vanilla.compiler.tester;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.tester.TesterContext;
import com.asakusafw.lang.compiler.tester.executor.TaskExecutor;
import com.asakusafw.lang.compiler.tester.executor.TaskExecutor.Context;
import com.asakusafw.lang.compiler.tester.executor.TaskExecutors;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.vanilla.compiler.common.VanillaTask;
import com.asakusafw.vanilla.compiler.tester.testing.SimpleApplication;

/**
 * Test for {@link InProcessVanillaTaskExecutor}.
 */
public class InProcessVanillaTaskExecutorTest {

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private final Map<String, String> arguments = new LinkedHashMap<>();

    private final Map<String, String> environmentVariables = new LinkedHashMap<>();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        TaskExecutor executor = new InProcessVanillaTaskExecutor();
        File home = folder.newFolder();
        Context context = context(home);
        putJar(context, jar -> {
            jar.putNextEntry(new ZipEntry(SimpleApplication.REQUIRED_FILE));
            jar.closeEntry();
        });
        CommandTaskReference task = command(SimpleApplication.class);
        assertThat(executor.isSupported(context, task), is(true));
        executor.execute(context, task);
    }

    private void putJar(Context context, Action<JarOutputStream, Exception> action) {
        File file = TaskExecutors.getJobflowLibrary(context);
        try (JarOutputStream output = new JarOutputStream(FileEditor.create(file))) {
            action.perform(output);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private CommandTaskReference command(Class<? extends Supplier<? extends GraphInfo>> application) {
        return new CommandTaskReference(
                VanillaTask.MODULE_NAME,
                VanillaTask.PROFILE_NAME,
                VanillaTask.PATH_COMMAND,
                Arrays.asList(new CommandToken[] {
                        CommandToken.BATCH_ID,
                        CommandToken.FLOW_ID,
                        CommandToken.EXECUTION_ID,
                        CommandToken.BATCH_ARGUMENTS,
                        CommandToken.of(application.getName()),
                }),
                Collections.emptyList());
    }

    private Context context(File home) {
        Map<String, String> env = new LinkedHashMap<>();
        env.putAll(environmentVariables);
        env.put(TesterContext.ENV_FRAMEWORK_PATH, home.getAbsolutePath());
        Context context = new Context(
                new TesterContext(getClass().getClassLoader(), env),
                new BatchInfo.Basic("BID", new ClassDescription("BID")),
                new JobflowInfo.Basic("FID", new ClassDescription("FID")),
                "EID",
                arguments);
        return context;
    }
}
