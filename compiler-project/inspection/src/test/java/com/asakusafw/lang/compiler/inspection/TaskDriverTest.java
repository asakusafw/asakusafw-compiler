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
package com.asakusafw.lang.compiler.inspection;

import static com.asakusafw.lang.compiler.api.reference.TaskReference.Phase.*;
import static com.asakusafw.lang.compiler.inspection.Util.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.basic.BasicBatchReference;
import com.asakusafw.lang.compiler.api.basic.BasicJobflowReference;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;
import com.asakusafw.lang.compiler.api.testing.MockTaskReferenceMap;
import com.asakusafw.lang.compiler.common.AttributeContainer;
import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.InspectionNode.Port;
import com.asakusafw.lang.inspection.InspectionNode.PortReference;

/**
 * Test for {@link TaskDriver}.
 */
public class TaskDriverTest {

    private final TaskDriver driver = new TaskDriver();

    /**
     * command task.
     */
    @Test
    public void task_command() {
        TaskReference ref = new CommandTaskReference(
                "testing",
                "local",
                Location.of("hello/world"),
                Arrays.asList(CommandToken.of("a"), CommandToken.EXECUTION_ID),
                Collections.emptySet(),
                Collections.emptyList());

        InspectionNode node = driver.inspect("t", ref);
        assertThat(node.getId(), is("t"));
    }

    /**
     * hadoop task.
     */
    @Test
    public void task_hadoop() {
        TaskReference ref = new HadoopTaskReference(
                new ClassDescription("testing"),
                Collections.emptyList());

        InspectionNode node = driver.inspect("t", ref);
        assertThat(node.getId(), is("t"));
    }

    /**
     * unknown task.
     */
    @Test
    public void task_unknown() {
        TaskReference ref = new MockTask() {
            // no members
        };
        InspectionNode node = driver.inspect("t", ref);
        assertThat(node.getId(), is("t"));
    }

    /**
     * jobflow - simple case.
     */
    @Test
    public void jobflow_simple() {
        TaskReference t0 = task();

        JobflowReference ref = jobflow("j", new MockTaskReferenceMap()
                .add(MAIN, t0));
        InspectionNode node = driver.inspect(ref);
        assertThat(node.getId(), is("j"));

        Map<TaskReference.Phase, InspectionNode> phases = phases(node);
        assertThat(phases.get(INITIALIZE).getElements().values(), hasSize(0));
        assertThat(phases.get(IMPORT).getElements().values(), hasSize(0));
        assertThat(phases.get(PROLOGUE).getElements().values(), hasSize(0));
        assertThat(phases.get(MAIN).getElements().values(), hasSize(1));
        assertThat(phases.get(EPILOGUE).getElements().values(), hasSize(0));
        assertThat(phases.get(EXPORT).getElements().values(), hasSize(0));
        assertThat(phases.get(FINALIZE).getElements().values(), hasSize(0));

        InspectionNode n0 = get(phases.get(MAIN), t0);
        assertThat(getPreds(n0), hasSize(0));
        assertThat(getSuccs(n0), hasSize(0));
    }

    /**
     * jobflow - all phases.
     */
    @Test
    public void jobflow_phases() {
        TaskReference t0 = task();
        TaskReference t1 = task();
        TaskReference t2 = task();
        TaskReference t3 = task();
        TaskReference t4 = task();
        TaskReference t5 = task();
        TaskReference t6 = task();

        JobflowReference ref = jobflow("j", new MockTaskReferenceMap()
                .add(INITIALIZE, t0)
                .add(IMPORT, t1)
                .add(PROLOGUE, t2)
                .add(MAIN, t3)
                .add(EPILOGUE, t4)
                .add(EXPORT, t5)
                .add(FINALIZE, t6));
        InspectionNode node = driver.inspect(ref);
        assertThat(node.getId(), is("j"));

        Map<TaskReference.Phase, InspectionNode> phases = phases(node);
        assertThat(phases.get(INITIALIZE).getElements().values(), hasSize(1));
        assertThat(phases.get(IMPORT).getElements().values(), hasSize(1));
        assertThat(phases.get(PROLOGUE).getElements().values(), hasSize(1));
        assertThat(phases.get(MAIN).getElements().values(), hasSize(1));
        assertThat(phases.get(EPILOGUE).getElements().values(), hasSize(1));
        assertThat(phases.get(EXPORT).getElements().values(), hasSize(1));
        assertThat(phases.get(FINALIZE).getElements().values(), hasSize(1));

        InspectionNode n0 = get(phases.get(INITIALIZE), t0);
        InspectionNode n1 = get(phases.get(IMPORT), t1);
        InspectionNode n2 = get(phases.get(PROLOGUE), t2);
        InspectionNode n3 = get(phases.get(MAIN), t3);
        InspectionNode n4 = get(phases.get(EPILOGUE), t4);
        InspectionNode n5 = get(phases.get(EXPORT), t5);
        InspectionNode n6 = get(phases.get(FINALIZE), t6);

        assertThat(getPreds(n0), hasSize(0));
        assertThat(getPreds(n1), hasSize(0));
        assertThat(getPreds(n2), hasSize(0));
        assertThat(getPreds(n3), hasSize(0));
        assertThat(getPreds(n4), hasSize(0));
        assertThat(getPreds(n5), hasSize(0));
        assertThat(getPreds(n6), hasSize(0));
        assertThat(getSuccs(n0), hasSize(0));
        assertThat(getSuccs(n1), hasSize(0));
        assertThat(getSuccs(n2), hasSize(0));
        assertThat(getSuccs(n3), hasSize(0));
        assertThat(getSuccs(n4), hasSize(0));
        assertThat(getSuccs(n5), hasSize(0));
        assertThat(getSuccs(n6), hasSize(0));
    }

    /**
     * jobflow - w/ diamond task graph.
     */
    @Test
    public void jobflow_diamond() {
        TaskReference t0 = task();
        TaskReference t1 = task(t0);
        TaskReference t2 = task(t0);
        TaskReference t3 = task(t1, t2);

        JobflowReference ref = jobflow("j", new MockTaskReferenceMap()
                .add(MAIN, t0, t1, t2, t3));
        InspectionNode node = driver.inspect(ref);
        assertThat(node.getId(), is("j"));

        Map<TaskReference.Phase, InspectionNode> phases = phases(node);
        assertThat(phases.get(INITIALIZE).getElements().values(), hasSize(0));
        assertThat(phases.get(IMPORT).getElements().values(), hasSize(0));
        assertThat(phases.get(PROLOGUE).getElements().values(), hasSize(0));
        assertThat(phases.get(MAIN).getElements().values(), hasSize(4));
        assertThat(phases.get(EPILOGUE).getElements().values(), hasSize(0));
        assertThat(phases.get(EXPORT).getElements().values(), hasSize(0));
        assertThat(phases.get(FINALIZE).getElements().values(), hasSize(0));

        InspectionNode n0 = get(phases.get(MAIN), t0);
        InspectionNode n1 = get(phases.get(MAIN), t1);
        InspectionNode n2 = get(phases.get(MAIN), t2);
        InspectionNode n3 = get(phases.get(MAIN), t3);

        assertThat(getPreds(n0), hasSize(0));
        assertThat(getPreds(n1), containsInAnyOrder(succ(n0)));
        assertThat(getPreds(n2), containsInAnyOrder(succ(n0)));
        assertThat(getPreds(n3), containsInAnyOrder(succ(n1), succ(n2)));

        assertThat(getSuccs(n0), containsInAnyOrder(pred(n1), pred(n2)));
        assertThat(getSuccs(n1), containsInAnyOrder(pred(n3)));
        assertThat(getSuccs(n2), containsInAnyOrder(pred(n3)));
        assertThat(getSuccs(n3), hasSize(0));
    }

    /**
     * batch - simple case.
     */
    @Test
    public void batch_simple() {
        JobflowReference j0 = jobflow("j0");

        BatchReference ref = batch("B", j0);
        InspectionNode node = driver.inspect(ref);
        assertThat(node.getId(), is("B"));

        InspectionNode n0 = node.getElements().get("j0");

        assertThat(n0, is(notNullValue()));
        assertThat(getPreds(n0), hasSize(0));
        assertThat(getSuccs(n0), hasSize(0));
    }

    /**
     * batch - w/ diamond jobflows.
     */
    @Test
    public void batch_diamond() {
        JobflowReference j0 = jobflow("j0");
        JobflowReference j1 = jobflow("j1", j0);
        JobflowReference j2 = jobflow("j2", j0);
        JobflowReference j3 = jobflow("j3", j1, j2);

        BatchReference ref = batch("B", j0, j1, j2, j3);
        InspectionNode node = driver.inspect(ref);
        assertThat(node.getId(), is("B"));

        InspectionNode n0 = node.getElements().get("j0");
        InspectionNode n1 = node.getElements().get("j1");
        InspectionNode n2 = node.getElements().get("j2");
        InspectionNode n3 = node.getElements().get("j3");

        assertThat(n0, is(notNullValue()));
        assertThat(n1, is(notNullValue()));
        assertThat(n2, is(notNullValue()));
        assertThat(n3, is(notNullValue()));

        assertThat(getPreds(n0), hasSize(0));
        assertThat(getPreds(n1), containsInAnyOrder(succ(n0)));
        assertThat(getPreds(n2), containsInAnyOrder(succ(n0)));
        assertThat(getPreds(n3), containsInAnyOrder(succ(n1), succ(n2)));

        assertThat(getSuccs(n0), containsInAnyOrder(pred(n1), pred(n2)));
        assertThat(getSuccs(n1), containsInAnyOrder(pred(n3)));
        assertThat(getSuccs(n2), containsInAnyOrder(pred(n3)));
        assertThat(getSuccs(n3), hasSize(0));
    }

    /**
     * jobflow - via {@link BasicObjectInspector}.
     */
    @Test
    public void jobflow_bridge() {
        TaskReference t0 = task();

        JobflowReference ref = jobflow("j", new MockTaskReferenceMap()
                .add(MAIN, t0));
        ObjectInspector inspector = new BasicObjectInspector();
        assertThat(inspector.isSupported(ref), is(true));
        InspectionNode node = inspector.inspect(ref);
        assertThat(node.getId(), is("j"));

        Map<TaskReference.Phase, InspectionNode> phases = phases(node);
        InspectionNode n0 = get(phases.get(MAIN), t0);
        assertThat(getPreds(n0), hasSize(0));
        assertThat(getSuccs(n0), hasSize(0));
    }

    /**
     * batch - via {@link BasicObjectInspector}.
     */
    @Test
    public void batch_via() {
        JobflowReference j0 = jobflow("j0");

        BatchReference ref = batch("B", j0);
        ObjectInspector inspector = new BasicObjectInspector();
        assertThat(inspector.isSupported(ref), is(true));
        InspectionNode node = inspector.inspect(ref);
        assertThat(node.getId(), is("B"));

        InspectionNode n0 = node.getElements().get("j0");

        assertThat(n0, is(notNullValue()));
        assertThat(getPreds(n0), hasSize(0));
        assertThat(getSuccs(n0), hasSize(0));
    }

    private InspectionNode get(InspectionNode node, AttributeContainer target) {
        Bless bless = target.getAttribute(Bless.class);
        assertThat(bless, is(notNullValue()));
        for (InspectionNode element : node.getElements().values()) {
            String value = element.getProperties().get(Util.getAttributeKey(Bless.class));
            if (value != null && value.equals(Util.getAttributeValue(bless))) {
                return element;
            }
        }
        throw new AssertionError(target);
    }

    private BatchReference batch(String id, JobflowReference... jobflows) {
        return new BasicBatchReference(
                new BatchInfo.Basic(id, new ClassDescription(id)),
                Arrays.asList(jobflows));
    }

    private JobflowReference jobflow(String id, JobflowReference... blockers) {
        return jobflow(id, new MockTaskReferenceMap().add(MAIN, task()), blockers);
    }

    private JobflowReference jobflow(String id, TaskReferenceMap tasks, JobflowReference... blockers) {
        return new BasicJobflowReference(
                new JobflowInfo.Basic(id, new ClassDescription(id)),
                tasks,
                Arrays.asList(blockers));
    }

    private Map<TaskReference.Phase, InspectionNode> phases(InspectionNode node) {
        Map<TaskReference.Phase, InspectionNode> results = new EnumMap<>(TaskReference.Phase.class);
        for (TaskReference.Phase phase : TaskReference.Phase.values()) {
            InspectionNode element = node.getElements().get(id(phase));
            assertThat(element, is(notNullValue()));
            results.put(phase, element);
        }
        assertThat(node.getElements().keySet(), hasSize(results.size()));
        return results;
    }

    private TaskReference task(TaskReference... blockers) {
        TaskReference ref = new CommandTaskReference(
                "testing",
                "local",
                Location.of("sh"),
                Collections.emptyList(),
                Collections.emptySet(),
                Arrays.asList(blockers));
        ref.putAttribute(Bless.class, new Bless());
        return ref;
    }

    private Set<PortReference> getPreds(InspectionNode node) {
        Port port = node.getInputs().get(Util.NAME_PREDECESSORS);
        assertThat(port, is(notNullValue()));
        return port.getOpposites();
    }

    private Set<PortReference> getSuccs(InspectionNode node) {
        Port port = node.getOutputs().get(Util.NAME_SUCCESSORS);
        assertThat(port, is(notNullValue()));
        return port.getOpposites();
    }

    private PortReference ref(String node, String port) {
        return new PortReference(node, port);
    }

    private PortReference succ(InspectionNode node) {
        return ref(node.getId(), Util.NAME_SUCCESSORS);
    }

    private PortReference pred(InspectionNode node) {
        return ref(node.getId(), Util.NAME_PREDECESSORS);
    }

    private static class Bless {

        static final AtomicInteger COUNTER = new AtomicInteger();

        final int index;

        Bless() {
            this.index = COUNTER.getAndIncrement();
        }

        @Override
        public String toString() {
            return String.format("Bless(%d)", index);
        }
    }

    private static class MockTask extends BasicAttributeContainer implements TaskReference {

        public MockTask() {
            return;
        }

        @Override
        public String getModuleName() {
            return "mock";
        }

        @Override
        public Set<String> getExtensions() {
            return Collections.emptySet();
        }

        @Override
        public Collection<? extends TaskReference> getBlockers() {
            return Collections.emptyList();
        }
    }
}
