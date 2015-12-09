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
package com.asakusafw.lang.compiler.tester.executor;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.tester.TesterContext;
import com.asakusafw.lang.compiler.tester.executor.TaskExecutor.Context;
import com.asakusafw.runtime.util.VariableTable;

/**
 * Test for {@link CommandTaskExecutor}.
 */
public class CommandTaskExecutorTest {

    private static final File BASH_COMMAND;
    static {
        File f = null;
        String path = System.getenv("PATH");
        if (path != null) {
            for (String s : path.split(Pattern.quote(File.pathSeparator))) {
                File file = new File(s, "bash");
                if (file.exists() && file.canExecute()) {
                    f = file;
                    break;
                }
            }
        }
        BASH_COMMAND = f;
    }

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private final Map<String, String> arguments = new LinkedHashMap<>();

    private final Map<String, String> environmentVariables = new LinkedHashMap<>();

    /**
     * initialize.
     */
    @Before
    public void initialize() {
        Assume.assumeThat("bash command is not found in current environment", BASH_COMMAND, is(notNullValue()));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        CommandTaskExecutor executor = new CommandTaskExecutor(BASH_COMMAND);

        File home = folder.newFolder();
        FileEditor.put(new File(home, "command.sh"),  new String[] {
            "echo 'CommandTaskExecutorTest.simple -> OK.'"
        });

        Context context = context(home);
        CommandTaskReference task = command("command.sh");
        executor.execute(context, task);
    }

    /**
     * unexpected exit code.
     * @throws Exception if failed
     */
    @Test(expected = IOException.class)
    public void unexpected_exit_code() throws Exception {
        CommandTaskExecutor executor = new CommandTaskExecutor(BASH_COMMAND);
        File home = folder.newFolder();
        FileEditor.put(new File(home, "command.sh"),  new String[] {
            "exit 1"
        });
        Context context = context(home);
        CommandTaskReference task = command("command.sh");
        executor.execute(context, task);
    }

    /**
     * command arguments - IDs.
     * @throws Exception if failed
     */
    @Test
    public void arguments_ids() throws Exception {
        CommandTaskExecutor executor = new CommandTaskExecutor(BASH_COMMAND);

        File home = folder.newFolder();
        File target = folder.newFile().getAbsoluteFile();
        FileEditor.put(new File(home, "command.sh"),  new String[] {
            String.format("echo \"$1\" >> '%s'", target),
            String.format("echo \"$2\" >> '%s'", target),
            String.format("echo \"$3\" >> '%s'", target),
        });

        Context context = context(home);
        CommandTaskReference task = command("command.sh", new CommandToken[] {
                CommandToken.BATCH_ID,
                CommandToken.FLOW_ID,
                CommandToken.EXECUTION_ID,
        });
        executor.execute(context, task);
        assertThat(FileEditor.get(target), contains("BID", "FID", "EID"));
    }

    /**
     * command arguments - batch arguments.
     * @throws Exception if failed
     */
    @Test
    public void arguments_batch_arguments() throws Exception {
        CommandTaskExecutor executor = new CommandTaskExecutor(BASH_COMMAND);

        File home = folder.newFolder();
        File target = folder.newFile().getAbsoluteFile();
        FileEditor.put(new File(home, "command.sh"),  new String[] {
            String.format("echo \"$1\" >> '%s'", target),
        });

        arguments.put("a", "A");
        arguments.put("b", "B");
        arguments.put("c", "C");
        Context context = context(home);
        CommandTaskReference task = command("command.sh", CommandToken.BATCH_ARGUMENTS);
        executor.execute(context, task);

        List<String> list = FileEditor.get(target);
        assertThat(list, hasSize(1));

        VariableTable table = new VariableTable();
        table.defineVariables(list.get(0));
        assertThat(table.getVariables(), is(arguments));
    }

    /**
     * pass environment variables.
     * @throws Exception if failed
     */
    @Test
    public void environment_variables() throws Exception {
        CommandTaskExecutor executor = new CommandTaskExecutor(BASH_COMMAND);

        File home = folder.newFolder();
        File target = folder.newFile().getAbsoluteFile();
        FileEditor.put(new File(home, "command.sh"),  new String[] {
            String.format("echo \"$a\" >> '%s'", target),
            String.format("echo \"$b\" >> '%s'", target),
            String.format("echo \"$c\" >> '%s'", target),
        });

        environmentVariables.put("a", "A");
        environmentVariables.put("b", "B");
        environmentVariables.put("c", "C");
        Context context = context(home);
        CommandTaskReference task = command("command.sh");
        executor.execute(context, task);
        assertThat(FileEditor.get(target), contains("A", "B", "C"));
    }

    private CommandTaskReference command(String path, CommandToken... args) {
        return new CommandTaskReference(
                "testing",
                "testing",
                Location.of(path),
                Arrays.asList(args),
                Collections.<String>emptySet(),
                Collections.<TaskReference>emptyList());
    }

    private Context context(File home) {
        Map<String, String> env = new LinkedHashMap<>();
        env.putAll(environmentVariables);
        env.put(TesterContext.ENV_FRAMEWORK_PATH, home.getAbsolutePath());
        return new Context(
                new TesterContext(getClass().getClassLoader(), env),
                new BatchInfo.Basic("BID", new ClassDescription("BID")),
                new JobflowInfo.Basic("FID", new ClassDescription("FID")),
                "EID",
                arguments);
    }
}
