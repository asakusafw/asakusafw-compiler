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
package com.asakusafw.lang.compiler.core.basic;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.basic.BasicBatchReference;
import com.asakusafw.lang.compiler.api.basic.BasicJobflowReference;
import com.asakusafw.lang.compiler.api.basic.JobflowContainer;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.CompilerParticipant;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.adapter.BatchProcessorAdapter;
import com.asakusafw.lang.compiler.core.util.FileContainerCleaner;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.BatchElement;
import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * A basic implementation of {@link BatchCompiler}.
 */
public class BasicBatchCompiler implements BatchCompiler {

    static final Logger LOG = LoggerFactory.getLogger(BasicBatchCompiler.class);

    private final JobflowCompiler jobflowCompiler;

    private final JobflowPackager jobflowPackager = new JobflowPackager();

    /**
     * Creates a new instance.
     */
    public BasicBatchCompiler() {
        this(new BasicJobflowCompiler());
    }

    /**
     * Creates a new instance.
     * @param jobflowCompiler the jobflow compiler
     */
    public BasicBatchCompiler(JobflowCompiler jobflowCompiler) {
        this.jobflowCompiler = jobflowCompiler;
    }

    @Override
    public void compile(Context context, Batch batch) {
        LOG.debug("start batch compiler: {}={}", batch.getBatchId(), batch.getDescriptionClass()); //$NON-NLS-1$
        before(context, batch);
        JobflowContainer container = new JobflowContainer();
        for (BatchElement element : sort(batch.getElements())) {
            TaskReferenceMap tasks = compileJobflow(context, element);
            container.add(new BasicJobflowReference(
                    element.getJobflow(),
                    tasks,
                    getBlockerJobflows(container, element)));
        }
        BatchReference reference = new BasicBatchReference(batch, container);
        runBatchProcessor(context, batch, reference);
        after(context, batch, reference);
    }

    private static List<BatchElement> sort(Set<BatchElement> elements) {
        Graph<BatchElement> graph = Graphs.newInstance();
        for (BatchElement element : elements) {
            graph.addNode(element);
            for (BatchElement blocker : element.getBlockerElements()) {
                graph.addEdge(element, blocker);
            }
        }
        List<BatchElement> sorted = Graphs.sortPostOrder(graph);
        return sorted;
    }

    private TaskReferenceMap compileJobflow(Context context, BatchElement element) {
        try (FileContainerCleaner cleaner = new FileContainerCleaner(createJobflowOutput(context, element))) {
            FileContainer jobflowOutput = cleaner.getContainer();
            JobflowCompiler.Context jobflowContext = new JobflowCompiler.Context(context, jobflowOutput);
            jobflowCompiler.compile(
                    jobflowContext,
                    element.getOwner(),
                    element.getJobflow());
            jobflowPackager.process(
                    element.getJobflow().getFlowId(),
                    context.getOutput(),
                    jobflowOutput,
                    context.getProject().getEmbeddedContents());
            return jobflowContext.getTaskContainerMap();
        } catch (IOException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "error occurred while compiling jobflow: batch={0}, jobflow={1}",
                    element.getOwner().getDescriptionClass().getClassName(),
                    element.getJobflow().getDescriptionClass().getClassName()), e);
        }
    }

    private static List<JobflowReference> getBlockerJobflows(JobflowContainer jobflows, BatchElement element) {
        List<JobflowReference> results = new ArrayList<>();
        for (BatchElement blocker : element.getBlockerElements()) {
            JobflowReference jobflow = jobflows.find(blocker.getJobflow().getFlowId());
            if (jobflow == null) {
                throw new IllegalStateException(blocker.getJobflow().toString());
            }
            results.add(jobflow);
        }
        return results;
    }

    private static FileContainer createJobflowOutput(Context context, BatchElement element) {
        String prefix = String.format("jobflow-%s", element.getJobflow().getFlowId()); //$NON-NLS-1$
        try {
            return context.getTemporaryOutputs().newContainer(prefix);
        } catch (IOException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to create temporary output: {0}",
                    context.getTemporaryOutputs().getRoot()), e);
        }
    }

    private static void runBatchProcessor(Context context, Batch batch, BatchReference reference) {
        BatchProcessorAdapter adapter = new BatchProcessorAdapter(context);
        BatchProcessor processor = context.getTools().getBatchProcessor();
        try {
            processor.process(adapter, reference);
        } catch (DiagnosticException e) {
            LOG.error(MessageFormat.format(
                    "error occurred while compiling batch: batch={0}",
                    batch.getDescriptionClass().getClassName()));
            throw e; // rethrow
        } catch (IOException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "error occurred while processing jobflow graph (batch={0})",
                    batch.getDescriptionClass().getClassName()), e);
        }
    }

    private static void before(Context context, Batch batch) {
        CompilerParticipant participant = context.getTools().getParticipant();
        participant.beforeBatch(context, batch);
    }

    private static void after(Context context, Batch batch, BatchReference reference) {
        CompilerParticipant participant = context.getTools().getParticipant();
        participant.afterBatch(context, batch, reference);
    }
}
