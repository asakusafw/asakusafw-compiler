/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.basic.BasicJobflowReference;
import com.asakusafw.lang.compiler.api.basic.TaskContainerMap;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.tester.ExternalPortMap;
import com.asakusafw.lang.compiler.tester.JobflowArtifact;
import com.asakusafw.lang.compiler.tester.TesterContext;

/**
 * Test for {@link JobflowExecutor}.
 */
public class JobflowExecutorTest {

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        DummyTaskExecutor tracker = new DummyTaskExecutor();
        JobflowExecutor executor = new JobflowExecutor(Collections.singletonList(tracker));

        TaskContainerMap tasks = new TaskContainerMap();
        TaskReference t0 = task("t0");
        tasks.getMainTaskContainer().add(t0);

        executor.execute(context(), jobflow(tasks));

        List<TaskReference> results = tracker.getTasks();
        assertThat(results, containsInAnyOrder(t0));
    }

    /**
     * loading from SPI.
     * @throws Exception if failed
     */
    @Test
    public void spi() throws Exception {
        TaskExecutor custom = new DummyTaskExecutor();
        JobflowExecutor executor = JobflowExecutor.newInstance(getClass().getClassLoader(), custom);

        List<TaskExecutor> elements = executor.getElementExecutors();
        assertThat(elements, hasSize(greaterThanOrEqualTo(2)));
        assertThat(elements.get(0), is(custom));
        assertThat(elements, hasItem(any(CommandTaskExecutor.class)));
    }

    /**
     * all phases.
     * @throws Exception if failed
     */
    @Test
    public void phases() throws Exception {
        DummyTaskExecutor tracker = new DummyTaskExecutor();
        JobflowExecutor executor = new JobflowExecutor(Collections.singletonList(tracker));

        TaskContainerMap tasks = new TaskContainerMap();
        TaskReference t0 = task("t0");
        TaskReference t1 = task("t1");
        TaskReference t2 = task("t2");
        TaskReference t3 = task("t3");
        TaskReference t4 = task("t4");
        TaskReference t5 = task("t5");
        TaskReference t6 = task("t6");
        tasks.getInitializeTaskContainer().add(t0);
        tasks.getImportTaskContainer().add(t1);
        tasks.getPrologueTaskContainer().add(t2);
        tasks.getMainTaskContainer().add(t3);
        tasks.getEpilogueTaskContainer().add(t4);
        tasks.getExportTaskContainer().add(t5);
        tasks.getFinalizeTaskContainer().add(t6);

        executor.execute(context(), jobflow(tasks));

        List<TaskReference> results = tracker.getTasks();
        assertThat(results, contains(t0, t1, t2, t3, t4, t5, t6));
    }

    /**
     * tasks w/ dependencies.
     * @throws Exception if failed
     */
    @Test
    public void dependencies() throws Exception {
        DummyTaskExecutor tracker = new DummyTaskExecutor();
        JobflowExecutor executor = new JobflowExecutor(Collections.singletonList(tracker));

        TaskContainerMap tasks = new TaskContainerMap();
        TaskReference t0 = task("t0");
        TaskReference t1 = task("t1", t0);
        TaskReference t2 = task("t2", t0);
        TaskReference t3 = task("t3", t1, t2);
        tasks.getMainTaskContainer().add(t0);
        tasks.getMainTaskContainer().add(t1);
        tasks.getMainTaskContainer().add(t2);
        tasks.getMainTaskContainer().add(t3);

        executor.execute(context(), jobflow(tasks));

        List<TaskReference> results = tracker.getTasks();
        assertThat(results, hasSize(4));
        assertThat(results, containsInAnyOrder(t0, t1, t2, t3));
        checkOrder(results, t0, t1);
        checkOrder(results, t0, t2);
        checkOrder(results, t1, t3);
        checkOrder(results, t2, t3);
    }

    /**
     * w/ hook actions.
     * @throws Exception if failed
     */
    @Test
    public void hooks() throws Exception {
        DummyTaskExecutor tracker = new DummyTaskExecutor();
        AtomicBoolean sawBefore = new AtomicBoolean();
        AtomicBoolean sawAfter = new AtomicBoolean();
        JobflowExecutor executor = new JobflowExecutor(Collections.singletonList(tracker));

        TaskContainerMap tasks = new TaskContainerMap();
        TaskReference t0 = task("t0");
        tasks.getMainTaskContainer().add(t0);

        executor.withBefore((context, artifact) -> {
            assertThat(tracker.getTasks(), is(empty()));
            assertThat(sawBefore.get(), is(false));
            sawBefore.set(true);
        })
        .withAfter((context, artifact) -> {
            assertThat(tracker.getTasks(), is(not(empty())));
            assertThat(sawAfter.get(), is(false));
            sawAfter.set(true);
        })
        .execute(context(), jobflow(tasks));

        assertThat(sawBefore.get(), is(true));
        assertThat(sawAfter.get(), is(true));
    }

    /**
     * w/o available task executors.
     * @throws Exception if failed
     */
    @Test(expected = IOException.class)
    public void no_availables() throws Exception {
        JobflowExecutor executor = new JobflowExecutor(Collections.singletonList(new TaskExecutor() {

            @Override
            public boolean isSupported(TaskExecutor.Context context, TaskReference task) {
                return false;
            }

            @Override
            public void execute(TaskExecutor.Context context, TaskReference task) {
                throw new AssertionError(task);
            }
        }));

        TaskContainerMap tasks = new TaskContainerMap();
        TaskReference t0 = task("t0");
        tasks.getMainTaskContainer().add(t0);

        executor.execute(context(), jobflow(tasks));
    }

    private void checkOrder(List<TaskReference> tasks, TaskReference pred, TaskReference succ) {
        int predIndex = tasks.indexOf(pred);
        int succIndex = tasks.indexOf(succ);
        assertThat(predIndex, is(greaterThanOrEqualTo(0)));
        assertThat(succIndex, is(greaterThanOrEqualTo(0)));
        assertThat(predIndex, is(lessThan(succIndex)));
    }

    private JobflowArtifact jobflow(TaskReferenceMap tasks) {
        BasicJobflowReference reference = new BasicJobflowReference(
                new JobflowInfo.Basic("FID", new ClassDescription("FID")),
                tasks,
                Collections.emptyList());
        return new JobflowArtifact(
                new BatchInfo.Basic("BID", new ClassDescription("BID")),
                reference,
                new ExternalPortMap());
    }

    private TaskReference task(String id, TaskReference... blockers) {
        return new CommandTaskReference(
                "testing",
                "testing",
                Location.of(id),
                Collections.emptyList(),
                Collections.emptySet(),
                Arrays.asList(blockers));
    }

    private TesterContext context() {
        try {
            return context(folder.newFolder());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private TesterContext context(File home) {
        return new TesterContext(getClass().getClassLoader(), Collections.emptyMap());
    }
}
