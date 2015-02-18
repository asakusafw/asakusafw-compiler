package com.asakusafw.lang.compiler.extension.windgate;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.basic.TaskContainer;
import com.asakusafw.lang.compiler.api.mock.MockExternalIoProcessorContext;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;
import com.asakusafw.vocabulary.windgate.Constants;
import com.asakusafw.vocabulary.windgate.WindGateExporterDescription;
import com.asakusafw.vocabulary.windgate.WindGateImporterDescription;
import com.asakusafw.windgate.core.DriverScript;
import com.asakusafw.windgate.core.GateScript;
import com.asakusafw.windgate.core.ProcessScript;
import com.asakusafw.windgate.core.vocabulary.FileProcess;

/**
 * Test for {@link WindGateIoProcessor}.
 */
public class WindGateIoProcessorTest {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * check module name.
     */
    @Test
    public void name() {
        WindGateIoProcessor proc = new WindGateIoProcessor();
        assertThat(proc.getModuleName(), is(WindGateIoProcessor.MODULE_NAME));
    }

    /**
     * check supported.
     */
    @Test
    public void supported() {
        MockExternalIoProcessorContext context = context();
        WindGateIoProcessor proc = new WindGateIoProcessor();
        assertThat(proc.isSupported(context, InputDesc.class), is(true));
        assertThat(proc.isSupported(context, OutputDesc.class), is(true));
        assertThat(proc.isSupported(context, ImporterDescription.class), is(false));
        assertThat(proc.isSupported(context, ExporterDescription.class), is(false));
        assertThat(proc.isSupported(context, String.class), is(false));
    }

    /**
     * input contents.
     */
    @Test
    public void input_contents() {
        InputDesc desc = new InputDesc(String.class, "p", script("r", "k0", "v0"));
        MockExternalIoProcessorContext context = context();
        WindGateIoProcessor proc = new WindGateIoProcessor();
        ValueDescription contents = proc.resolveInputProperties(context, "t", desc);
        DescriptionModel model = restore(contents);

        assertThat(model.getProfileName(), is("p"));
        assertThat(model.getDriverScript().getResourceName(), is("r"));
        assertThat(model.getDriverScript().getConfiguration().keySet(), hasSize(1));
        assertThat(model.getDriverScript().getConfiguration(), hasEntry("k0", "v0"));
    }

    /**
     * output contents.
     */
    @Test
    public void output_contents() {
        OutputDesc desc = new OutputDesc(String.class, "p", script("r", "k0", "v0"));
        MockExternalIoProcessorContext context = context();
        WindGateIoProcessor proc = new WindGateIoProcessor();
        ValueDescription contents = proc.resolveOutputProperties(context, "t", desc);
        DescriptionModel model = restore(contents);

        assertThat(model.getProfileName(), is("p"));
        assertThat(model.getDriverScript().getResourceName(), is("r"));
        assertThat(model.getDriverScript().getConfiguration().keySet(), hasSize(1));
        assertThat(model.getDriverScript().getConfiguration(), hasEntry("k0", "v0"));
    }

    /**
     * invalid input.
     */
    @Test(expected = DiagnosticException.class)
    public void resolve_input_invalid() {
        InputDesc desc = new InputDesc(String.class, "p", null);
        MockExternalIoProcessorContext context = context();
        WindGateIoProcessor proc = new WindGateIoProcessor();
        proc.resolveInput(context, "n", desc);
    }

    /**
     * invalid output.
     */
    @Test(expected = DiagnosticException.class)
    public void resolve_output_invalid() {
        OutputDesc desc = new OutputDesc(String.class, "p", null);
        MockExternalIoProcessorContext context = context();
        WindGateIoProcessor proc = new WindGateIoProcessor();
        proc.resolveOutput(context, "n", desc);
    }

    /**
     * processes input.
     * @throws Exception if failed
     */
    @Test
    public void process_input() throws Exception {
        InputDesc desc = new InputDesc(String.class, "p", script("r", "id", "p0"));
        MockExternalIoProcessorContext context = context();
        WindGateIoProcessor proc = new WindGateIoProcessor();
        proc.process(context, inputs(desc), outputs());

        File scriptFile = context.getOutputFile(WindGateIoProcessor.getImportScriptLocation("p"));
        assertThat(scriptFile.isFile(), is(true));
        GateScript script = load(scriptFile);
        assertThat(script.getProcesses(), hasSize(1));

        ProcessScript<?> p0 = findProcess(script, "p0", DriverScript.Kind.SOURCE);
        DriverScript s0 = p0.getSourceScript();
        assertThat(s0.getResourceName(), is("r"));
        assertThat(s0.getConfiguration(), hasEntry("id", "p0"));

        DriverScript d0 = p0.getDrainScript();
        assertThat(d0.getResourceName(), is(Constants.HADOOP_FILE_RESOURCE_NAME));
        assertThat(d0.getConfiguration(), hasEntry(FileProcess.FILE.key(), "p0"));

        assertThat(context.getTasks().getTasks(Phase.IMPORT), hasSize(1));
        assertThat(context.getTasks().getTasks(Phase.EXPORT), hasSize(0));
        assertThat(context.getTasks().getTasks(Phase.FINALIZE), hasSize(1));
        checkTask(context.getTasks().getImportTaskContainer(), "p", WindGateIoProcessor.OPT_ONESHOT);
        checkFinalize(context.getTasks().getFinalizeTaskContainer(), "p");
    }

    /**
     * processes output.
     * @throws Exception if failed
     */
    @Test
    public void process_output() throws Exception {
        OutputDesc desc = new OutputDesc(String.class, "p", script("r", "id", "p0"));
        MockExternalIoProcessorContext context = context();
        WindGateIoProcessor proc = new WindGateIoProcessor();
        proc.process(context, inputs(), outputs(desc));

        File scriptFile = context.getOutputFile(WindGateIoProcessor.getExportScriptLocation("p"));
        assertThat(scriptFile.isFile(), is(true));
        GateScript script = load(scriptFile);
        assertThat(script.getProcesses(), hasSize(1));

        ProcessScript<?> p0 = findProcess(script, "p0", DriverScript.Kind.DRAIN);
        DriverScript s0 = p0.getSourceScript();
        assertThat(s0.getResourceName(), is(Constants.HADOOP_FILE_RESOURCE_NAME));
        assertThat(s0.getConfiguration(), hasEntry(FileProcess.FILE.key(), "p0"));

        DriverScript d0 = p0.getDrainScript();
        assertThat(d0.getResourceName(), is("r"));
        assertThat(d0.getConfiguration(), hasEntry("id", "p0"));

        assertThat(context.getTasks().getTasks(Phase.IMPORT), hasSize(0));
        assertThat(context.getTasks().getTasks(Phase.EXPORT), hasSize(1));
        assertThat(context.getTasks().getTasks(Phase.FINALIZE), hasSize(1));
        checkTask(context.getTasks().getExportTaskContainer(), "p", WindGateIoProcessor.OPT_ONESHOT);
        checkFinalize(context.getTasks().getFinalizeTaskContainer(), "p");
    }

    /**
     * processes output.
     * @throws Exception if failed
     */
    @Test
    public void process_multiple() throws Exception {
        InputDesc in0 = new InputDesc(String.class, "p0", script("r", "id", "p0"));
        InputDesc in1 = new InputDesc(String.class, "p1", script("r", "id", "p1"));
        OutputDesc out0 = new OutputDesc(String.class, "p1", script("r", "id", "p1"));
        OutputDesc out1 = new OutputDesc(String.class, "p2", script("r", "id", "p2"));
        MockExternalIoProcessorContext context = context();
        WindGateIoProcessor proc = new WindGateIoProcessor();
        proc.process(context, inputs(in0, in1), outputs(out0, out1));

        assertThat(context.getOutputFile(WindGateIoProcessor.getImportScriptLocation("p0")).exists(), is(true));
        assertThat(context.getOutputFile(WindGateIoProcessor.getImportScriptLocation("p1")).exists(), is(true));
        assertThat(context.getOutputFile(WindGateIoProcessor.getImportScriptLocation("p2")).exists(), is(false));

        assertThat(context.getOutputFile(WindGateIoProcessor.getExportScriptLocation("p0")).exists(), is(false));
        assertThat(context.getOutputFile(WindGateIoProcessor.getExportScriptLocation("p1")).exists(), is(true));
        assertThat(context.getOutputFile(WindGateIoProcessor.getExportScriptLocation("p2")).exists(), is(true));

        assertThat(context.getTasks().getTasks(Phase.IMPORT), hasSize(2));
        assertThat(context.getTasks().getTasks(Phase.EXPORT), hasSize(2));
        assertThat(context.getTasks().getTasks(Phase.FINALIZE), hasSize(3));

        checkTask(context.getTasks().getImportTaskContainer(), "p0", WindGateIoProcessor.OPT_ONESHOT);
        checkTask(context.getTasks().getImportTaskContainer(), "p1", WindGateIoProcessor.OPT_BEGIN);

        checkTask(context.getTasks().getExportTaskContainer(), "p1", WindGateIoProcessor.OPT_END);
        checkTask(context.getTasks().getExportTaskContainer(), "p2", WindGateIoProcessor.OPT_ONESHOT);

        checkFinalize(context.getTasks().getFinalizeTaskContainer(), "p0");
        checkFinalize(context.getTasks().getFinalizeTaskContainer(), "p1");
        checkFinalize(context.getTasks().getFinalizeTaskContainer(), "p2");
    }

    private ProcessScript<?> findProcess(GateScript script, String id, DriverScript.Kind kind) {
        for (ProcessScript<?> process : script.getProcesses()) {
            DriverScript driver = process.getDriverScript(kind);
            String value = driver.getConfiguration().get("id");
            if (value != null && value.equals(id)) {
                return process;
            }
        }
        throw new AssertionError(id);
    }

    private GateScript load(File scriptFile) {
        try (InputStream in = new FileInputStream(scriptFile)) {
            Properties props = new Properties();
            props.load(in);
            return GateScript.loadFrom("testing", props, getClass().getClassLoader());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private DescriptionModel restore(ValueDescription contents) {
        try {
            Object resolved = contents.resolve(DescriptionModel.class.getClassLoader());
            assertThat(resolved, is(instanceOf(DescriptionModel.class)));
            return (DescriptionModel) resolved;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private List<ExternalInputReference> inputs(ImporterDescription... descriptions) {
        try {
            MockExternalIoProcessorContext context = context();
            WindGateIoProcessor proc = new WindGateIoProcessor();
            List<ExternalInputReference> results = new ArrayList<>();
            int index = 0;
            for (ImporterDescription desc : descriptions) {
                String name = String.format("p%d", index++);
                ExternalInputInfo info = proc.resolveInput(context, name, desc);
                results.add(new ExternalInputReference(name, info, Collections.singleton(name)));
            }
            return results;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private List<ExternalOutputReference> outputs(ExporterDescription... descriptions) {
        try {
            MockExternalIoProcessorContext context = context();
            WindGateIoProcessor proc = new WindGateIoProcessor();
            List<ExternalOutputReference> results = new ArrayList<>();
            int index = 0;
            for (ExporterDescription desc : descriptions) {
                String name = String.format("p%d", index++);
                ExternalOutputInfo info = proc.resolveOutput(context, name, desc);
                results.add(new ExternalOutputReference(name, info, Collections.singleton(name)));
            }
            return results;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private MockExternalIoProcessorContext context() {
        return new MockExternalIoProcessorContext(
                new CompilerOptions(
                        "testing",
                        "rwd",
                        Collections.<String, String>emptyMap()),
                getClass().getClassLoader(),
                temporary.getRoot());
    }

    private DriverScript script(String resourceName, String... keyValuePairs) {
        assertThat(keyValuePairs.length % 2, is(0));
        Map<String, String> configuration = new HashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            configuration.put(keyValuePairs[i + 0], keyValuePairs[i + 1]);
        }
        return new DriverScript(resourceName, configuration);
    }

    @SuppressWarnings("unchecked")
    private void checkTask(TaskContainer tasks, String profile, String kind) {
        CommandTaskReference task = findTask(tasks, profile);
        assertThat(task.getModuleName(), is(WindGateIoProcessor.MODULE_NAME));
        assertThat(task.getProfileName(), is(profile));
        assertThat(task.getCommand(), is(WindGateIoProcessor.CMD_PROCESS));
        assertThat(task.getArguments(), contains(
                is(CommandToken.of(profile)),
                is(CommandToken.of(kind)),
                any(CommandToken.class),
                is(CommandToken.BATCH_ID),
                is(CommandToken.FLOW_ID),
                is(CommandToken.EXECUTION_ID),
                is(CommandToken.BATCH_ARGUMENTS)
        ));
    }

    @SuppressWarnings("unchecked")
    private void checkFinalize(TaskContainer tasks, String profile) {
        CommandTaskReference task = findTask(tasks, profile);
        assertThat(task.getModuleName(), is(WindGateIoProcessor.MODULE_NAME));
        assertThat(task.getProfileName(), is(profile));
        assertThat(task.getCommand(), is(WindGateIoProcessor.CMD_FINALIZE));
        assertThat(task.getArguments(), contains(
                is(CommandToken.of(profile)),
                is(CommandToken.BATCH_ID),
                is(CommandToken.FLOW_ID),
                is(CommandToken.EXECUTION_ID)
        ));
    }

    private CommandTaskReference findTask(TaskContainer tasks, String profile) {
        CommandTaskReference candidate = null;
        for (TaskReference task : tasks.getElements()) {
            if (task instanceof CommandTaskReference) {
                CommandTaskReference cmd = (CommandTaskReference) task;
                if (cmd.getProfileName().equals(profile)) {
                    assertThat(profile, candidate, is(nullValue()));
                    candidate = cmd;
                }
            }
        }
        assertThat(profile, candidate, is(notNullValue()));
        return candidate;
    }

    private static class InputDesc extends WindGateImporterDescription {

        private final Class<?> modelType;

        private final String profileName;

        private final DriverScript driverScript;

        public InputDesc(Class<?> modelType, String profileName, DriverScript driverScript) {
            this.modelType = modelType;
            this.profileName = profileName;
            this.driverScript = driverScript;
        }

        @Override
        public Class<?> getModelType() {
            return modelType;
        }

        @Override
        public String getProfileName() {
            return profileName;
        }

        @Override
        public DriverScript getDriverScript() {
            return driverScript;
        }
    }

    private static class OutputDesc extends WindGateExporterDescription {

        private final Class<?> modelType;

        private final String profileName;

        private final DriverScript driverScript;

        public OutputDesc(Class<?> modelType, String profileName, DriverScript driverScript) {
            this.modelType = modelType;
            this.profileName = profileName;
            this.driverScript = driverScript;
        }

        @Override
        public Class<?> getModelType() {
            return modelType;
        }

        @Override
        public String getProfileName() {
            return profileName;
        }

        @Override
        public DriverScript getDriverScript() {
            return driverScript;
        }
    }
}
