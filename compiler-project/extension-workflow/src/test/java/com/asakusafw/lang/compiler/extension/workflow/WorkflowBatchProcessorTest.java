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
package com.asakusafw.lang.compiler.extension.workflow;

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
import java.util.stream.Collectors;

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
import com.asakusafw.workflow.model.CommandTaskInfo;
import com.asakusafw.workflow.model.HadoopTaskInfo;
import com.asakusafw.workflow.model.TaskInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test for {@link WorkflowBatchProcessor}.
 */
public class WorkflowBatchProcessorTest {

    private static final String DUMMY_CMD = "dummy/command.sh";

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

        com.asakusafw.workflow.model.BatchInfo script = execute(context, batch);
        assertThat(script.getId(), is("b"));
        assertThat(script.getElements(), hasSize(1));

        com.asakusafw.workflow.model.JobflowInfo f0 = script.findElement("f0").get();
        assertThat(f0, is(notNullValue()));
        assertThat(f0.getId(), is("f0"));
        assertThat(f0.getBlockers(), is(empty()));
        assertThat(f0.getTasks(TaskInfo.Phase.CLEANUP), hasSize(not(0)));

        assertThat(getExplicitTasks(f0), hasSize(1));
        List<TaskInfo> f0main = list(f0.getTasks(TaskInfo.Phase.MAIN));
        assertThat(f0main, hasSize(1));

        TaskInfo t0 = f0main.get(0);
        assertThat(t0, is(instanceOf(CommandTaskInfo.class)));
        assertThat(t0.getBlockers(), is(empty()));
        CommandTaskInfo t0c = (CommandTaskInfo) t0;
        assertThat(t0c.getCommand(), is(DUMMY_CMD));
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
        com.asakusafw.workflow.model.BatchInfo script = execute(batch);
        assertThat(script.getElements(), hasSize(4));

        com.asakusafw.workflow.model.JobflowInfo f0 = script.findElement("f0").get();
        assertThat(f0, is(notNullValue()));
        assertThat(f0.getId(), is("f0"));
        assertThat(f0.getBlockers(), is(empty()));
        assertThat(f0.getTasks(TaskInfo.Phase.CLEANUP), hasSize(not(0)));

        com.asakusafw.workflow.model.JobflowInfo f1 = script.findElement("f1").get();
        assertThat(f1, is(notNullValue()));
        assertThat(f1.getId(), is("f1"));
        assertThat(f1.getBlockers(), containsInAnyOrder(f0));
        assertThat(f1.getTasks(TaskInfo.Phase.CLEANUP), hasSize(not(0)));

        com.asakusafw.workflow.model.JobflowInfo f2 = script.findElement("f2").get();
        assertThat(f2, is(notNullValue()));
        assertThat(f2.getId(), is("f2"));
        assertThat(f2.getBlockers(), containsInAnyOrder(f0));
        assertThat(f1.getTasks(TaskInfo.Phase.CLEANUP), hasSize(not(0)));

        com.asakusafw.workflow.model.JobflowInfo f3 = script.findElement("f3").get();
        assertThat(f3, is(notNullValue()));
        assertThat(f3.getId(), is("f3"));
        assertThat(f3.getBlockers(), containsInAnyOrder(f1, f2));
        assertThat(f1.getTasks(TaskInfo.Phase.CLEANUP), hasSize(not(0)));
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
        com.asakusafw.workflow.model.BatchInfo script = execute(batch);
        assertThat(script.getElements(), hasSize(1));
        com.asakusafw.workflow.model.JobflowInfo f0 = script.findElement("f0").get();
        assertThat(f0, is(notNullValue()));
        assertThat(f0.getTasks(TaskInfo.Phase.CLEANUP), hasSize(not(0)));

        assertThat(getExplicitTasks(f0), hasSize(7));

        List<TaskInfo> p0 = list(f0.getTasks(TaskInfo.Phase.INITIALIZE));
        assertThat(p0, hasSize(1));
        assertThat(p0.get(0), is(instanceOf(CommandTaskInfo.class)));
        assertThat(((CommandTaskInfo) p0.get(0)).getModuleName(), is("t0"));

        List<TaskInfo> p1 = list(f0.getTasks(TaskInfo.Phase.IMPORT));
        assertThat(p1, hasSize(1));
        assertThat(p1.get(0), is(instanceOf(CommandTaskInfo.class)));
        assertThat(((CommandTaskInfo) p1.get(0)).getModuleName(), is("t1"));

        List<TaskInfo> p2 = list(f0.getTasks(TaskInfo.Phase.PROLOGUE));
        assertThat(p2, hasSize(1));
        assertThat(p2.get(0), is(instanceOf(CommandTaskInfo.class)));
        assertThat(((CommandTaskInfo) p2.get(0)).getModuleName(), is("t2"));

        List<TaskInfo> p3 = list(f0.getTasks(TaskInfo.Phase.MAIN));
        assertThat(p3, hasSize(1));
        assertThat(p3.get(0), is(instanceOf(CommandTaskInfo.class)));
        assertThat(((CommandTaskInfo) p3.get(0)).getModuleName(), is("t3"));

        List<TaskInfo> p4 = list(f0.getTasks(TaskInfo.Phase.EPILOGUE));
        assertThat(p4, hasSize(1));
        assertThat(p4.get(0), is(instanceOf(CommandTaskInfo.class)));
        assertThat(((CommandTaskInfo) p4.get(0)).getModuleName(), is("t4"));

        List<TaskInfo> p5 = list(f0.getTasks(TaskInfo.Phase.EXPORT));
        assertThat(p5, hasSize(1));
        assertThat(p5.get(0), is(instanceOf(CommandTaskInfo.class)));
        assertThat(((CommandTaskInfo) p5.get(0)).getModuleName(), is("t5"));

        List<TaskInfo> p6 = list(f0.getTasks(TaskInfo.Phase.FINALIZE));
        assertThat(p6, hasSize(1));
        assertThat(p6.get(0), is(instanceOf(CommandTaskInfo.class)));
        assertThat(((CommandTaskInfo) p6.get(0)).getModuleName(), is("t6"));
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
        com.asakusafw.workflow.model.BatchInfo script = execute(batch);
        assertThat(script.getElements(), hasSize(1));

        com.asakusafw.workflow.model.JobflowInfo f0 = script.findElement("f0").get();
        assertThat(f0, is(notNullValue()));
        assertThat(f0.getId(), is("f0"));
        assertThat(f0.getBlockers(), is(empty()));
        assertThat(f0.getTasks(TaskInfo.Phase.CLEANUP), hasSize(not(0)));
        assertThat(getExplicitTasks(f0), hasSize(4));

        List<TaskInfo> p0 = list(f0.getTasks(TaskInfo.Phase.MAIN));
        assertThat(p0, hasSize(4));
        CommandTaskInfo t0 = find(p0, "t0");
        assertThat(t0.getBlockers(), is(empty()));

        CommandTaskInfo t1 = find(p0, "t1");
        assertThat(t1.getBlockers(), containsInAnyOrder(t0));

        CommandTaskInfo t2 = find(p0, "t2");
        assertThat(t2.getBlockers(), containsInAnyOrder(t0));

        CommandTaskInfo t3 = find(p0, "t3");
        assertThat(t3.getBlockers(), containsInAnyOrder(t1, t2));
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

        com.asakusafw.workflow.model.BatchInfo script = execute(context, batch);
        com.asakusafw.workflow.model.JobflowInfo f = script.findElement("F").get();
        assertThat(f.getTasks(TaskInfo.Phase.CLEANUP), hasSize(not(0)));
        CommandTaskInfo cmd = find(getExplicitTasks(f), "m");

        assertThat(cmd.getModuleName(), is("m"));
        assertThat(cmd.getProfileName(), is("P"));
        assertThat(cmd.getCommand(), is("CMD"));
        assertThat(cmd.getArguments(), contains(
                com.asakusafw.workflow.model.CommandToken.of("T"), // text
                com.asakusafw.workflow.model.CommandToken.BATCH_ID,
                com.asakusafw.workflow.model.CommandToken.FLOW_ID,
                com.asakusafw.workflow.model.CommandToken.EXECUTION_ID,
                com.asakusafw.workflow.model.CommandToken.BATCH_ARGUMENTS));
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

        com.asakusafw.workflow.model.BatchInfo script = execute(batch);
        com.asakusafw.workflow.model.JobflowInfo f = script.findElement("F").get();
        assertThat(f.getTasks(TaskInfo.Phase.CLEANUP), hasSize(not(0)));
        assertThat(getExplicitTasks(f), hasSize(1));

        TaskInfo s0 = getExplicitTasks(f).get(0);
        assertThat(s0, is(instanceOf(HadoopTaskInfo.class)));

        HadoopTaskInfo h0 = (HadoopTaskInfo) s0;
        assertThat(h0.getClassName(), is("HADOOP"));
    }

    /**
     * w/o hadoop command.
     */
    @Test
    public void no_hadoop_required() {
        JobflowReference sf0 = jobflow("f0", new MockTaskReferenceMap()
                .add(Phase.MAIN, HadoopCommandRequired.put(task("t0"), false)));
        BatchReference batch = batch("b", sf0);
        com.asakusafw.workflow.model.BatchInfo script = execute(batch);
        assertThat(script.getElements(), hasSize(1));

        com.asakusafw.workflow.model.JobflowInfo f0 = script.findElement("f0").get();
        assertThat(f0, is(notNullValue()));
        assertThat(f0.getId(), is("f0"));
        assertThat(f0.getBlockers(), is(empty()));
        assertThat(f0.getTasks(TaskInfo.Phase.CLEANUP), hasSize(0));
    }

    private List<TaskInfo> getExplicitTasks(com.asakusafw.workflow.model.JobflowInfo jobflow) {
        return Arrays.stream(TaskInfo.Phase.values())
                .filter(it -> it != TaskInfo.Phase.CLEANUP)
                .map(jobflow::getTasks)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private <T> List<T> list(Collection<? extends T> collection) {
        return new ArrayList<>(collection);
    }

    private CommandTaskInfo find(Collection<? extends TaskInfo> scripts, String moduleName) {
        return scripts.stream()
                .filter(it -> it instanceof CommandTaskInfo)
                .map(it -> (CommandTaskInfo) it)
                .filter(it -> it.getModuleName().equals(moduleName))
                .findFirst()
                .orElseThrow(() -> new AssertionError(moduleName));
    }

    private com.asakusafw.workflow.model.BatchInfo execute(BatchReference batch) {
        MockBatchProcessorContext context = new MockBatchProcessorContext(
                CompilerOptions.builder().build(),
                getClass().getClassLoader(),
                temporary.getRoot());
        return execute(context, batch);
    }

    private com.asakusafw.workflow.model.BatchInfo execute(MockBatchProcessorContext context, BatchReference batch) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            BatchProcessor processor = new WorkflowBatchProcessor();
            processor.process(context, batch);
            File script = new File(context.getBaseDirectory(), WorkflowBatchProcessor.PATH);
            try (InputStream in = new FileInputStream(script)) {
                return mapper.readValue(in, com.asakusafw.workflow.model.BatchInfo.class);
            }
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
