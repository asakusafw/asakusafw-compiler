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

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.tester.BatchArtifact;
import com.asakusafw.lang.compiler.tester.JobflowArtifact;
import com.asakusafw.lang.compiler.tester.TesterContext;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Executes a batch for testing.
 */
public class BatchExecutor implements ArtifactExecutor<BatchArtifact> {

    static final Logger LOG = LoggerFactory.getLogger(BatchExecutor.class);

    private final JobflowExecutor elementExecutor;

    private final List<Action> preActions = new ArrayList<>();

    private final List<Action> postActions = new ArrayList<>();

    /**
     * Creates a new instance.
     * @param jobflowExecutor the jobflow executor
     */
    public BatchExecutor(JobflowExecutor jobflowExecutor) {
        this.elementExecutor = jobflowExecutor;
    }

    /**
     * Adds an action which will be performed before executing element jobflows.
     * @param action the action
     * @return this
     */
    public BatchExecutor withBefore(Action action) {
        preActions.add(action);
        return this;
    }

    /**
     * Adds an action which will be performed after executing element jobflows.
     * Note that, these action will not be performed when jobflow executions were failed.
     * @param action the action
     * @return this
     */
    public BatchExecutor withAfter(Action action) {
        postActions.add(action);
        return this;
    }

    @Override
    public void execute(TesterContext context, BatchArtifact artifact) throws InterruptedException, IOException {
        execute(context, artifact, Collections.<String, String>emptyMap());
    }

    @Override
    public void execute(
            TesterContext context,
            BatchArtifact artifact,
            Map<String, String> arguments) throws IOException, InterruptedException {
        BatchExecutor.Context wrapped = new BatchExecutor.Context(context, arguments);
        execute(wrapped, artifact);
    }

    private void execute(Context context, BatchArtifact artifact) throws IOException, InterruptedException {
        LOG.info(MessageFormat.format(
                "start executing batch: batch={0}",
                artifact.getReference().getBatchId()));
        Set<JobflowArtifact> elements = artifact.getJobflows();
        List<JobflowArtifact> sorted = sortElements(elements);
        for (Action action : preActions) {
            action.perform(context, artifact);
        }
        for (JobflowArtifact element : sorted) {
            executeElement(context, element);
        }
        for (Action action : postActions) {
            action.perform(context, artifact);
        }
    }

    private void executeElement(Context context, JobflowArtifact artifact) throws InterruptedException, IOException {
        elementExecutor.execute(context.getTesterContext(), artifact);
    }

    private List<JobflowArtifact> sortElements(Set<JobflowArtifact> elements) {
        Map<JobflowReference, JobflowArtifact> artifacts = new HashMap<>();
        for (JobflowArtifact element : elements) {
            artifacts.put(element.getReference(), element);
        }
        Graph<JobflowArtifact> graph = Graphs.newInstance();
        for (JobflowArtifact element : elements) {
            graph.addNode(element);
            for (JobflowReference blocker : element.getReference().getBlockers()) {
                JobflowArtifact blockerArtifact = artifacts.get(blocker);
                assert blockerArtifact != null;
                graph.addEdge(element, blockerArtifact);
            }
        }
        List<JobflowArtifact> results = Graphs.sortPostOrder(graph);
        return results;
    }

    /**
     * Represents an action for {@link BatchExecutor}.
     */
    public interface Action {

        /**
         * Performs an action for {@link BatchArtifact}.
         * @param context the current execution context
         * @param artifact the target artifact
         * @throws IOException if failed to execute the target artifact
         * @throws InterruptedException if interrupted while executing the artifact
         */
        void perform(Context context, BatchArtifact artifact) throws InterruptedException, IOException;
    }

    /**
     * Represents a context of {@link JobflowExecutor}.
     */
    public static final class Context {

        private final TesterContext testerContext;

        private final Map<String, String> batchArguments;

        /**
         * Creates a new instance.
         * @param testerContext the parent tester context
         * @param batchArguments the batch arguments
         */
        public Context(TesterContext testerContext, Map<String, String> batchArguments) {
            this.testerContext = testerContext;
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
         * Returns the batch arguments.
         * @return the batch arguments
         */
        public Map<String, String> getBatchArguments() {
            return batchArguments;
        }
    }
}
