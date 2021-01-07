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
package com.asakusafw.lang.compiler.tester.executor;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.tester.JobflowArtifact;
import com.asakusafw.lang.compiler.tester.TesterContext;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Executes a jobflow for testing.
 */
public class JobflowExecutor implements ArtifactExecutor<JobflowArtifact> {

    static final Logger LOG = LoggerFactory.getLogger(JobflowExecutor.class);

    private final List<TaskExecutor> elementExecutors;

    private final List<Action> preActions = new ArrayList<>();

    private final List<Action> postActions = new ArrayList<>();

    /**
     * Creates a new instance.
     * @param taskExecutors the task executors in descending order according to their priority
     */
    public JobflowExecutor(Collection<? extends TaskExecutor> taskExecutors) {
        this.elementExecutors = Collections.unmodifiableList(new ArrayList<>(taskExecutors));
    }

    /**
     * Creates a new instance with default task executors.
     * @param serviceLoader the class loader for obtaining default task executors
     *     in descending order according to their priority
     * @param customExecutors the custom task executors
     * @return the created instance
     */
    public static JobflowExecutor newInstance(ClassLoader serviceLoader, TaskExecutor... customExecutors) {
        List<TaskExecutor> executors = new ArrayList<>();
        Collections.addAll(executors, customExecutors);
        executors.addAll(TaskExecutors.loadDefaults(serviceLoader));
        return new JobflowExecutor(executors);
    }

    /**
     * Returns the task executors in this.
     * @return the task executors
     */
    public List<TaskExecutor> getElementExecutors() {
        return elementExecutors;
    }

    /**
     * Adds an action which will be performed before executing element tasks.
     * @param action the action
     * @return this
     */
    public JobflowExecutor withBefore(Action action) {
        preActions.add(action);
        return this;
    }

    /**
     * Adds an action which will be performed after executing element tasks.
     * Note that, these action will not be performed when task executions were failed.
     * @param action the action
     * @return this
     */
    public JobflowExecutor withAfter(Action action) {
        postActions.add(action);
        return this;
    }

    @Override
    public void execute(
            TesterContext context,
            JobflowArtifact artifact,
            Map<String, String> arguments) throws InterruptedException, IOException {
        JobflowExecutor.Context wrapped = new JobflowExecutor.Context(context, artifact.getBatch(), arguments);
        execute(wrapped, artifact);
    }

    private void execute(Context context, JobflowArtifact artifact) throws InterruptedException, IOException {
        LOG.info(MessageFormat.format(
                "start executing jobflow: batch={0}, jobflow={1}",
                context.getBatch().getBatchId(),
                artifact.getReference().getFlowId()));
        for (Action action : preActions) {
            action.perform(context, artifact);
        }
        TaskExecutor.Context elementContext = createElementContext(context, artifact.getReference());
        for (TaskReference.Phase phase : TaskReference.Phase.values()) {
            Collection<? extends TaskReference> elements = artifact.getReference().getTasks(phase);
            if (elements.isEmpty()) {
                continue;
            }
            List<TaskReference> sorted = sortElements(elements);
            for (TaskReference element : sorted) {
                executeElement(elementContext, element);
            }
        }
        for (Action action : postActions) {
            action.perform(context, artifact);
        }
    }

    private TaskExecutor.Context createElementContext(Context context, JobflowInfo current) {
        String executionId = UUID.randomUUID().toString();
        return new TaskExecutor.Context(
                context.getTesterContext(),
                context.getBatch(),
                current,
                executionId,
                context.getBatchArguments());
    }

    private void executeElement(
            TaskExecutor.Context context, TaskReference element) throws IOException, InterruptedException {
        for (TaskExecutor elementExecutor : elementExecutors) {
            if (elementExecutor.isSupported(context, element)) {
                elementExecutor.execute(context, element);
                return;
            }
        }
        throw new IOException(MessageFormat.format(
                "no available task executor: batch={0}, jobflow={1}, task={2}, executors={3}",
                context.getBatch().getBatchId(),
                context.getJobflow().getFlowId(),
                element.getModuleName(),
                elementExecutors));
    }

    private List<TaskReference> sortElements(Collection<? extends TaskReference> elements) {
        Graph<TaskReference> graph = Graphs.newInstance();
        for (TaskReference element : elements) {
            graph.addEdges(element, element.getBlockers());
        }
        List<TaskReference> results = Graphs.sortPostOrder(graph);
        return results;
    }

    /**
     * Represents an action for {@link JobflowExecutor}.
     */
    @FunctionalInterface
    public interface Action {

        /**
         * Performs an action for {@link JobflowArtifact}.
         * @param context the current execution context
         * @param artifact the target artifact
         * @throws IOException if failed to execute the target artifact
         * @throws InterruptedException if interrupted while executing the artifact
         */
        void perform(Context context, JobflowArtifact artifact) throws InterruptedException, IOException;
    }

    /**
     * Represents a context of {@link JobflowExecutor}.
     */
    public static final class Context {

        private final TesterContext testerContext;

        private final BatchInfo batch;

        private final Map<String, String> batchArguments;

        /**
         * Creates a new instance.
         * @param testerContext the parent tester context
         * @param batch the container batch information
         * @param batchArguments the batch arguments
         */
        public Context(TesterContext testerContext, BatchInfo batch, Map<String, String> batchArguments) {
            this.testerContext = testerContext;
            this.batch = batch;
            this.batchArguments = batchArguments;
        }

        /**
         * Returns the current tester context.
         * @return the current tester context
         */
        public TesterContext getTesterContext() {
            return testerContext;
        }

        /**
         * Returns the container batch information.
         * @return the container batch information
         */
        public BatchInfo getBatch() {
            return batch;
        }

        /**
         * Returns the batch arguments.
         * @return the batch arguments
         */
        public Map<String, String> getBatchArguments() {
            return batchArguments;
        }
    }
}
