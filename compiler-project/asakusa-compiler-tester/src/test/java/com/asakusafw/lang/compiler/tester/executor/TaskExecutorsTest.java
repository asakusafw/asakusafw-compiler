package com.asakusafw.lang.compiler.tester.executor;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.core.basic.JobflowPackager;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.tester.TesterContext;
import com.asakusafw.lang.compiler.tester.executor.TaskExecutor.Context;
import com.asakusafw.runtime.util.VariableTable;

/**
 * Test for {@link TaskExecutors}.
 */
public class TaskExecutorsTest {

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private final Map<String, String> arguments = new LinkedHashMap<>();

    private final Map<String, String> environmentVariables = new LinkedHashMap<>();

    /**
     * obtain framework file.
     * @throws Exception if failed
     */
    @Test
    public void framework_file() throws Exception {
        File home = folder.newFolder();
        FileEditor.put(new File(home, "testing/file.txt"), "Hello, world!");

        Context context = context(home);
        File file = TaskExecutors.getFrameworkFile(context, Location.of("testing/file.txt"));
        assertThat(FileEditor.get(file), contains("Hello, world!"));
    }

    /**
     * obtain application file.
     * @throws Exception if failed
     */
    @Test
    public void application_file() throws Exception {
        File home = folder.newFolder();
        File batchapps = new File(home, TesterContext.DEFAULT_BATCHAPPS_PATH);
        File application = new File(batchapps, "BID");
        FileEditor.put(new File(application, "testing/file.txt"), "Hello, world!");

        Context context = context(home);
        File file = TaskExecutors.getApplicationFile(context, Location.of("testing/file.txt"));
        assertThat(FileEditor.get(file), contains("Hello, world!"));
    }

    /**
     * resolve command token - immediate.
     */
    @Test
    public void resolve_command_token_text() {
        Context context = context();
        String resolved = TaskExecutors.resolveCommandToken(context, CommandToken.of("Hello, world!"));
        assertThat(resolved, is("Hello, world!"));
    }

    /**
     * resolve command token - IDs.
     */
    @Test
    public void resolve_command_ids() {
        Context context = context();
        List<String> resolved = TaskExecutors.resolveCommandTokens(context, Arrays.asList(
                CommandToken.BATCH_ID,
                CommandToken.FLOW_ID,
                CommandToken.EXECUTION_ID));
        assertThat(resolved, contains("BID", "FID", "EID"));
    }

    /**
     * resolve command token - batch arguments.
     */
    @Test
    public void resolve_command_arguments() {
        arguments.put("a", "A");
        arguments.put("b", "B");
        arguments.put("c", "C");
        Context context = context();
        String resolved = TaskExecutors.resolveCommandToken(context, CommandToken.BATCH_ARGUMENTS);

        VariableTable table = new VariableTable();
        table.defineVariables(resolved);
        assertThat(table.getVariables(), is(arguments));
    }

    /**
     * obtain jobflow library file.
     * @throws Exception if failed
     */
    @Test
    public void jobflow_library() throws Exception {
        File home = folder.newFolder();
        File batchapps = new File(home, TesterContext.DEFAULT_BATCHAPPS_PATH);
        File application = new File(batchapps, "BID");
        FileEditor.put(new File(application, JobflowPackager.getLibraryLocation("FID").toPath()), "Hello, world!");

        Context context = context(home);
        File file = TaskExecutors.getJobflowLibrary(context);
        assertThat(FileEditor.get(file), contains("Hello, world!"));
    }

    /**
     * obtain attached library files.
     * @throws Exception if failed
     */
    @Test
    public void attached_libraries() throws Exception {
        File home = folder.newFolder();
        File batchapps = new File(home, TesterContext.DEFAULT_BATCHAPPS_PATH);
        File application = new File(batchapps, "BID");
        FileEditor.put(new File(application, "usr/lib/a.jar"), "A");
        FileEditor.put(new File(application, "usr/lib/b.jar"), "B");
        FileEditor.put(new File(application, "usr/lib/c.txt"), "C");
        FileEditor.put(new File(application, "usr/lib/deep/d.txt"), "D");

        Context context = context(home);
        Set<String> contents = new LinkedHashSet<>();
        for (File file : TaskExecutors.getAttachedLibraries(context)) {
            contents.addAll(FileEditor.get(file));
        }
        assertThat(contents, containsInAnyOrder("A", "B"));
    }

    /**
     * obtain framework library files.
     * @throws Exception if failed
     */
    @Test
    public void framework_libraries() throws Exception {
        File home = folder.newFolder();
        FileEditor.put(new File(home, "testing/lib/a.jar"), "A");
        FileEditor.put(new File(home, "testing/lib/b.jar"), "B");
        FileEditor.put(new File(home, "testing/lib/c.txt"), "C");
        FileEditor.put(new File(home, "testing/lib/deep/d.txt"), "D");

        Context context = context(home);
        Set<String> contents = new LinkedHashSet<>();
        for (File file : TaskExecutors.getFrameworkLibraries(context, Location.of("testing/lib"))) {
            contents.addAll(FileEditor.get(file));
        }
        assertThat(contents, containsInAnyOrder("A", "B"));

        assertThat(TaskExecutors.getFrameworkLibraries(context, Location.of("other")), hasSize(0));
    }

    /**
     * obtain core library files.
     * @throws Exception if failed
     */
    @Test
    public void core_libraries() throws Exception {
        File home = folder.newFolder();
        FileEditor.put(new File(home, "core/lib/a.jar"), "A");
        FileEditor.put(new File(home, "core/lib/b.jar"), "B");
        FileEditor.put(new File(home, "core/lib/c.txt"), "C");
        FileEditor.put(new File(home, "core/lib/deep/d.txt"), "D");

        Context context = context(home);
        Set<String> contents = new LinkedHashSet<>();
        for (File file : TaskExecutors.getCoreLibraries(context)) {
            contents.addAll(FileEditor.get(file));
        }
        assertThat(contents, containsInAnyOrder("A", "B"));
    }

    /**
     * obtain core configuration file.
     * @throws Exception if failed
     */
    @Test
    public void core_configuration() throws Exception {
        File home = folder.newFolder();
        FileEditor.put(new File(home, "core/conf/asakusa-resources.xml"), "<!-- Hello, world! -->");

        Context context = context(home);
        File file = TaskExecutors.getCoreConfigurationFile(context);
        assertThat(FileEditor.get(file), contains("<!-- Hello, world! -->"));
    }

    /**
     * obtain extension library files.
     * @throws Exception if failed
     */
    @Test
    public void extension_libraries() throws Exception {
        File home = folder.newFolder();
        FileEditor.put(new File(home, "ext/lib/a.jar"), "A");
        FileEditor.put(new File(home, "ext/lib/b.jar"), "B");
        FileEditor.put(new File(home, "ext/lib/c.txt"), "C");
        FileEditor.put(new File(home, "ext/lib/deep/d.txt"), "D");

        Context context = context(home);
        Set<String> contents = new LinkedHashSet<>();
        for (File file : TaskExecutors.getExtensionLibraries(context)) {
            contents.addAll(FileEditor.get(file));
        }
        assertThat(contents, containsInAnyOrder("A", "B"));
    }

    private Context context() {
        try {
            return context(folder.newFolder());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
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
