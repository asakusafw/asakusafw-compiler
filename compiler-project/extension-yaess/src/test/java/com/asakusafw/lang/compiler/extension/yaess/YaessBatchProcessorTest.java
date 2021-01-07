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
package com.asakusafw.lang.compiler.extension.yaess;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.basic.BasicBatchReference;
import com.asakusafw.lang.compiler.api.basic.BasicJobflowReference;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;
import com.asakusafw.lang.compiler.api.testing.MockBatchProcessorContext;
import com.asakusafw.lang.compiler.api.testing.MockTaskReferenceMap;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.hadoop.HadoopCommandRequired;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.yaess.core.BatchScript;
import com.asakusafw.yaess.core.CommandScript;
import com.asakusafw.yaess.core.ExecutionPhase;
import com.asakusafw.yaess.core.ExecutionScript;
import com.asakusafw.yaess.core.FlowScript;
import com.asakusafw.yaess.core.HadoopScript;

/**
 * Test for {@link YaessBatchProcessor}.
 */
public class YaessBatchProcessorTest {

    private static final String DUMMY_CMD = String.format(
            "%s/%s",
            ExecutionScript.PLACEHOLDER_HOME,
            "dummy/command.sh");

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockBatchProcessorContext context = new MockBatchProcessorContext(
                CompilerOptions.builder().withBuildId("TESTING").build(),
                getClass().getClassLoader(),
                temporary.getRoot());

        MockTaskReferenceMap tasks = new MockTaskReferenceMap()
            .add(Phase.MAIN, task("t"));
        BatchReference batch = batch("b", jobflow("f0", tasks));

        BatchScript script = execute(context, batch);
        assertThat(script.getBuildId(), is("TESTING"));
        assertThat(script.getId(), is("b"));
        assertThat(script.getAllFlows(), hasSize(1));

        FlowScript f0 = script.findFlow("f0");
        assertThat(f0, is(notNullValue()));
        assertThat(f0.getId(), is("f0"));
        assertThat(f0.getBlockerIds(), is(empty()));
        assertThat(f0.getEnabledScriptKinds(), hasItem(ExecutionScript.Kind.HADOOP));

        assertThat(flatten(f0.getScripts()), hasSize(1));
        List<ExecutionScript> f0main = list(f0.getScripts().get(ExecutionPhase.MAIN));
        assertThat(f0main, hasSize(1));

        ExecutionScript t0 = f0main.get(0);
        assertThat(t0.getKind(), is(ExecutionScript.Kind.COMMAND));
        assertThat(t0.getBlockerIds(), is(empty()));
        CommandScript t0c = (CommandScript) t0;
        assertThat(t0c.getCommandLineTokens(), contains(DUMMY_CMD));
    }

    /**
     * many jobflows.
     */
    @Test
    public void many_jobflows() {
        /*
         * f0 +-- f1 --+ f3
         *     \- f2 -/
         */
        JobflowReference sf0 = jobflow("f0", new MockTaskReferenceMap().add(Phase.MAIN, task("t0")));
        JobflowReference sf1 = jobflow("f1", new MockTaskReferenceMap().add(Phase.MAIN, task("t1")), sf0);
        JobflowReference sf2 = jobflow("f2", new MockTaskReferenceMap().add(Phase.MAIN, task("t2")), sf0);
        JobflowReference sf3 = jobflow("f3", new MockTaskReferenceMap().add(Phase.MAIN, task("t3")), sf1, sf2);
        BatchReference batch = batch("b", sf0, sf1, sf2, sf3);
        BatchScript script = execute(batch);
        assertThat(script.getAllFlows(), hasSize(4));

        FlowScript f0 = script.findFlow("f0");
        assertThat(f0, is(notNullValue()));
        assertThat(f0.getId(), is("f0"));
        assertThat(f0.getBlockerIds(), is(empty()));
        assertThat(f0.getEnabledScriptKinds(), hasItem(ExecutionScript.Kind.HADOOP));

        FlowScript f1 = script.findFlow("f1");
        assertThat(f1, is(notNullValue()));
        assertThat(f1.getId(), is("f1"));
        assertThat(f1.getBlockerIds(), containsInAnyOrder("f0"));
        assertThat(f1.getEnabledScriptKinds(), hasItem(ExecutionScript.Kind.HADOOP));

        FlowScript f2 = script.findFlow("f2");
        assertThat(f2, is(notNullValue()));
        assertThat(f2.getId(), is("f2"));
        assertThat(f2.getBlockerIds(), containsInAnyOrder("f0"));
        assertThat(f1.getEnabledScriptKinds(), hasItem(ExecutionScript.Kind.HADOOP));

        FlowScript f3 = script.findFlow("f3");
        assertThat(f3, is(notNullValue()));
        assertThat(f3.getId(), is("f3"));
        assertThat(f3.getBlockerIds(), containsInAnyOrder("f1", "f2"));
        assertThat(f1.getEnabledScriptKinds(), hasItem(ExecutionScript.Kind.HADOOP));
    }

    /**
     * many phases.
     */
    @Test
    public void many_phases() {
        JobflowReference sf0 = jobflow("f0", new MockTaskReferenceMap()
            .add(Phase.INITIALIZE, task("t0"))
            .add(Phase.IMPORT, task("t1"))
            .add(Phase.PROLOGUE, task("t2"))
            .add(Phase.MAIN, task("t3"))
            .add(Phase.EPILOGUE, task("t4"))
            .add(Phase.EXPORT, task("t5"))
            .add(Phase.FINALIZE, task("t6")));

        BatchReference batch = batch("b", sf0);
        BatchScript script = execute(batch);
        assertThat(script.getAllFlows(), hasSize(1));
        FlowScript f0 = script.findFlow("f0");
        assertThat(f0, is(notNullValue()));
        assertThat(f0.getEnabledScriptKinds(), hasItem(ExecutionScript.Kind.HADOOP));

        assertThat(flatten(f0.getScripts()), hasSize(7));

        List<ExecutionScript> p0 = list(f0.getScripts().get(ExecutionPhase.INITIALIZE));
        assertThat(p0, hasSize(1));
        assertThat(p0.get(0).getKind(), is(ExecutionScript.Kind.COMMAND));
        assertThat(((CommandScript) p0.get(0)).getModuleName(), is("t0"));

        List<ExecutionScript> p1 = list(f0.getScripts().get(ExecutionPhase.IMPORT));
        assertThat(p1, hasSize(1));
        assertThat(p1.get(0).getKind(), is(ExecutionScript.Kind.COMMAND));
        assertThat(((CommandScript) p1.get(0)).getModuleName(), is("t1"));

        List<ExecutionScript> p2 = list(f0.getScripts().get(ExecutionPhase.PROLOGUE));
        assertThat(p2, hasSize(1));
        assertThat(p2.get(0).getKind(), is(ExecutionScript.Kind.COMMAND));
        assertThat(((CommandScript) p2.get(0)).getModuleName(), is("t2"));

        List<ExecutionScript> p3 = list(f0.getScripts().get(ExecutionPhase.MAIN));
        assertThat(p3, hasSize(1));
        assertThat(p3.get(0).getKind(), is(ExecutionScript.Kind.COMMAND));
        assertThat(((CommandScript) p3.get(0)).getModuleName(), is("t3"));

        List<ExecutionScript> p4 = list(f0.getScripts().get(ExecutionPhase.EPILOGUE));
        assertThat(p4, hasSize(1));
        assertThat(p4.get(0).getKind(), is(ExecutionScript.Kind.COMMAND));
        assertThat(((CommandScript) p4.get(0)).getModuleName(), is("t4"));

        List<ExecutionScript> p5 = list(f0.getScripts().get(ExecutionPhase.EXPORT));
        assertThat(p5, hasSize(1));
        assertThat(p5.get(0).getKind(), is(ExecutionScript.Kind.COMMAND));
        assertThat(((CommandScript) p5.get(0)).getModuleName(), is("t5"));

        List<ExecutionScript> p6 = list(f0.getScripts().get(ExecutionPhase.FINALIZE));
        assertThat(p6, hasSize(1));
        assertThat(p6.get(0).getKind(), is(ExecutionScript.Kind.COMMAND));
        assertThat(((CommandScript) p6.get(0)).getModuleName(), is("t6"));
    }

    /**
     * many tasks.
     */
    @Test
    public void many_tasks() {
        /*
         * t0 +-- t1 --+ t3
         *     \- t2 -/
         */
        TaskReference st0 = task("t0");
        TaskReference st1 = task("t1", st0);
        TaskReference st2 = task("t2", st0);
        TaskReference st3 = task("t3", st1, st2);
        JobflowReference sf0 = jobflow("f0", new MockTaskReferenceMap().add(Phase.MAIN, st0, st1, st2, st3));
        BatchReference batch = batch("b", sf0);
        BatchScript script = execute(batch);
        assertThat(script.getAllFlows(), hasSize(1));

        FlowScript f0 = script.findFlow("f0");
        assertThat(f0, is(notNullValue()));
        assertThat(f0.getId(), is("f0"));
        assertThat(f0.getBlockerIds(), is(empty()));
        assertThat(f0.getEnabledScriptKinds(), hasItem(ExecutionScript.Kind.HADOOP));
        assertThat(flatten(f0.getScripts()), hasSize(4));

        List<ExecutionScript> p0 = list(f0.getScripts().get(ExecutionPhase.MAIN));
        assertThat(p0, hasSize(4));
        CommandScript t0 = find(p0, "t0");
        assertThat(t0.getBlockerIds(), is(empty()));

        CommandScript t1 = find(p0, "t1");
        assertThat(t1.getBlockerIds(), containsInAnyOrder(t0.getId()));

        CommandScript t2 = find(p0, "t2");
        assertThat(t2.getBlockerIds(), containsInAnyOrder(t0.getId()));

        CommandScript t3 = find(p0, "t3");
        assertThat(t3.getBlockerIds(), containsInAnyOrder(t1.getId(), t2.getId()));
    }

    /**
     * command script.
     */
    @Test
    public void command() {
        MockBatchProcessorContext context = new MockBatchProcessorContext(
                CompilerOptions.builder().build(),
                getClass().getClassLoader(),
                temporary.getRoot());

        MockTaskReferenceMap tasks = new MockTaskReferenceMap()
            .add(Phase.MAIN, new CommandTaskReference(
                    "m",
                    "P",
                    Location.of("CMD"),
                    Arrays.asList(new CommandToken[] {
                            CommandToken.of("T"),
                            CommandToken.BATCH_ID,
                            CommandToken.FLOW_ID,
                            CommandToken.EXECUTION_ID,
                            CommandToken.BATCH_ARGUMENTS,
                    }),
                    Arrays.asList("e1", "e2"),
                    Collections.emptyList()));
        BatchReference batch = batch("B", jobflow("F", tasks));

        BatchScript script = execute(context, batch);
        FlowScript f = script.findFlow("F");
        assertThat(f.getEnabledScriptKinds(), hasItem(ExecutionScript.Kind.HADOOP));
        CommandScript cmd = find(flatten(f.getScripts()), "m");

        assertThat(cmd.getModuleName(), is("m"));
        assertThat(cmd.getProfileName(), is("P"));
        assertThat(cmd.getCommandLineTokens(), contains(
                String.format("%s/CMD", ExecutionScript.PLACEHOLDER_HOME),
                "T", // text
                "B", // batch ID
                "F", // flow ID,
                ExecutionScript.PLACEHOLDER_EXECUTION_ID,
                ExecutionScript.PLACEHOLDER_ARGUMENTS));
        assertThat(cmd.getSupportedExtensions(), containsInAnyOrder("e1", "e2"));
        assertThat(cmd.getEnvironmentVariables().keySet(), is(empty()));
    }

    /**
     * hadoop script.
     */
    @Test
    public void hadoop() {
        MockTaskReferenceMap tasks = new MockTaskReferenceMap()
            .add(Phase.MAIN, new HadoopTaskReference(
                    new ClassDescription("HADOOP"),
                    Arrays.asList("e1", "e2"),
                    Collections.emptyList()));
        BatchReference batch = batch("B", jobflow("F", tasks));

        BatchScript script = execute(batch);
        FlowScript f = script.findFlow("F");
        assertThat(f.getEnabledScriptKinds(), hasItem(ExecutionScript.Kind.HADOOP));
        assertThat(flatten(f.getScripts()), hasSize(1));

        ExecutionScript s0 = flatten(f.getScripts()).get(0);
        assertThat(s0.getKind(), is(ExecutionScript.Kind.HADOOP));
        assertThat(s0.getSupportedExtensions(), containsInAnyOrder("e1", "e2"));

        HadoopScript h0 = (HadoopScript) s0;
        assertThat(h0.getClassName(), is("HADOOP"));
        assertThat(h0.getEnvironmentVariables().keySet(), is(empty()));
    }

    /**
     * w/o hadoop command.
     */
    @Test
    public void no_hadoop_required() {
        JobflowReference sf0 = jobflow("f0", new MockTaskReferenceMap()
                .add(Phase.MAIN, HadoopCommandRequired.put(task("t0"), false)));
        BatchReference batch = batch("b", sf0);
        BatchScript script = execute(batch);
        assertThat(script.getAllFlows(), hasSize(1));

        FlowScript f0 = script.findFlow("f0");
        assertThat(f0, is(notNullValue()));
        assertThat(f0.getId(), is("f0"));
        assertThat(f0.getBlockerIds(), is(empty()));
        assertThat(f0.getEnabledScriptKinds(), not(hasItem(ExecutionScript.Kind.HADOOP)));
    }

    private <T> List<T> list(Collection<? extends T> collection) {
        return new ArrayList<>(collection);
    }

    private <T> List<T> flatten(Map<?, ? extends Collection<? extends T>> map) {
        List<T> results = new ArrayList<>();
        for (Collection<? extends T> values : map.values()) {
            results.addAll(values);
        }
        return results;
    }

    private CommandScript find(Collection<? extends ExecutionScript> scripts, String moduleName) {
        for (ExecutionScript script : scripts) {
            if (script instanceof CommandScript) {
                if (((CommandScript) script).getModuleName().equals(moduleName)) {
                    return (CommandScript) script;
                }
            }
        }
        throw new AssertionError(moduleName);
    }

    private BatchScript execute(BatchReference batch) {
        MockBatchProcessorContext context = new MockBatchProcessorContext(
                CompilerOptions.builder().build(),
                getClass().getClassLoader(),
                temporary.getRoot());
        return execute(context, batch);
    }

    private BatchScript execute(MockBatchProcessorContext context, BatchReference batch) {
        try {
            BatchProcessor processor = new YaessBatchProcessor();
            processor.process(context, batch);
            File script = YaessBatchProcessor.getScriptOutput(context.getBaseDirectory());
            Properties properties = new Properties();
            try (InputStream in = new FileInputStream(script)) {
                properties.load(in);
            }
            return BatchScript.load(properties);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private BatchReference batch(String id, JobflowReference... jobflows) {
        return new BasicBatchReference(
                new BatchInfo.Basic(id, new ClassDescription("Dummy")),
                Arrays.asList(jobflows));
    }

    private JobflowReference jobflow(String id, TaskReferenceMap tasks, JobflowReference... blockers) {
        return new BasicJobflowReference(
                new JobflowInfo.Basic(id, new ClassDescription("Dummy")),
                tasks,
                Arrays.asList(blockers));
    }

    private CommandTaskReference task(String module, TaskReference... blockers) {
        return task(module, Collections.emptyList(), blockers);
    }

    private CommandTaskReference task(String module, List<? extends CommandToken> args, TaskReference... blockers) {
        return new CommandTaskReference(
                module,
                "dummy",
                Location.of("dummy/command.sh"),
                args,
                Collections.emptySet(),
                Arrays.asList(blockers));
    }
}
