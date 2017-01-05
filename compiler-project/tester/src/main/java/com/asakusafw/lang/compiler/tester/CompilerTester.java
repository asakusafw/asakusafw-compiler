/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.tester;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.BatchCompiler.Context;
import com.asakusafw.lang.compiler.core.ClassAnalyzer;
import com.asakusafw.lang.compiler.core.CompilerContext;
import com.asakusafw.lang.compiler.core.CompilerParticipant;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.ProjectRepository;
import com.asakusafw.lang.compiler.core.ToolRepository;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.core.basic.BasicBatchCompiler;
import com.asakusafw.lang.compiler.core.util.CompositeClassAnalyzer;
import com.asakusafw.lang.compiler.core.util.CompositeCompilerParticipant;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.lang.compiler.packaging.ResourceSink;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;
import com.asakusafw.lang.compiler.tester.executor.util.DummyBatchClass;

/**
 * A compiler tester.
 * <p>
 * This object has lifecycle for managing temporary files and their corresponded resources.
 * When the object was {@link #close() closed}, the following contents becomes unavailable:
 * </p>
 * <ul>
 * <li> {@link ProjectRepository} object for the current session </li>
 * <li>
 *   {@link TesterContext#removeTemporaryFiles() temporary directories} for the current session:
 *   this may include compiler working directories, <em>volatile framework installations</em>, and
 *   compiler deliverables.
 * </li>
 * </ul>
 * <p>
 * For this, each test executions for the compilation result, must be done before closing the object,
 * as like:
 * </p>
<pre><code>
BatchExecutor executor = ...;
try (CompilerTester compiler = ...) {
    // first, compile the target DSL element
    BatchArtifact artifact = compiler.compile(...);

    // prepare testing environment
    ...

    // and then execute the compilation result
    executor.execute(compiler.getTesterContext(), artifact);

    // finally, verify the execution results
    ...
}
// session was disposed: the compilation deliverables may be lost
</code></pre>
 * <p>
 * Clients can use {@link CompilerProfile} for configuring this object.
 * </p>
 * @see CompilerProfile
 * @see BatchArtifact
 * @see JobflowArtifact
 * @since 0.1.0
 * @version 0.3.0
 */
public class CompilerTester implements Closeable {

    static final Logger LOG = LoggerFactory.getLogger(CompilerTester.class);

    private final ClassAnalyzer classAnalyzer;

    private final BatchCompiler batchCompiler;

    private final TesterContext testerContext;

    private final CompilerContext compilerContext;

    private final ResultCollector collector;

    /**
     * Creates a new instance.
     * @param testerContext the tester context
     * @param compilerContext the compiler context
     */
    public CompilerTester(TesterContext testerContext, CompilerContext compilerContext) {
        this(new CompositeClassAnalyzer(), new BasicBatchCompiler(), testerContext, compilerContext);
    }

    /**
     * Creates a new instance.
     * @param classAnalyzer the class analyzer
     * @param batchCompiler the batch compiler
     * @param testerContext the tester context
     * @param compilerContext the compiler context
     */
    public CompilerTester(
            ClassAnalyzer classAnalyzer, BatchCompiler batchCompiler,
            TesterContext testerContext, CompilerContext compilerContext) {
        this.classAnalyzer = classAnalyzer;
        this.batchCompiler = batchCompiler;
        this.testerContext = testerContext;
        this.collector = new ResultCollector();
        this.compilerContext = enhance(compilerContext, collector);
    }

    private static CompilerContext enhance(CompilerContext base, ResultCollector collector) {
        return new CompilerContext.Basic(
                base.getOptions(),
                base.getProject(),
                enhance(base.getTools(), collector),
                base.getTemporaryOutputs());
    }

    private static ToolRepository enhance(ToolRepository tools, ResultCollector collector) {
        return new ToolRepository(
                tools.getDataModelProcessor(),
                tools.getBatchProcessor(),
                tools.getJobflowProcessor(),
                tools.getExternalPortProcessor(),
                enhance(tools.getParticipant(), collector));
    }

    private static CompilerParticipant enhance(CompilerParticipant participant, ResultCollector collector) {
        List<CompilerParticipant> elements = new ArrayList<>();
        if (participant instanceof CompositeCompilerParticipant) {
            CompositeCompilerParticipant composite = (CompositeCompilerParticipant) participant;
            elements.addAll(composite.getElements());
        } else {
            elements.add(participant);
        }
        elements.add(collector);
        return CompositeCompilerParticipant.composite(elements);
    }

    /**
     * Returns the current compiler context.
     * @return the current compiler context
     */
    public CompilerContext getCompilerContext() {
        return compilerContext;
    }

    /**
     * Returns the current tester context.
     * @return the current tester context
     */
    public TesterContext getTesterContext() {
        return testerContext;
    }

    /**
     * Analyzes a jobflow class.
     * @param jobflowClass the target jobflow class
     * @return the analyzed model
     * @throws DiagnosticException if the target jobflow class is not valid
     */
    public Jobflow analyzeJobflow(Class<?> jobflowClass) {
        return classAnalyzer.analyzeJobflow(new ClassAnalyzer.Context(compilerContext), jobflowClass);
    }

    /**
     * Analyzes a batch class.
     * @param batchClass the target batch class
     * @return the analyzed model
     * @throws DiagnosticException if the target batch class is not valid
     */
    public Batch analyzeBatch(Class<?> batchClass) {
        return classAnalyzer.analyzeBatch(new ClassAnalyzer.Context(compilerContext), batchClass);
    }

    /**
     * Compiles the target jobflow.
     * @param jobflow the target jobflow
     * @return the compilation result
     * @throws IOException if failed to prepare the compilation environment
     * @throws DiagnosticException if failed to compile the target jobflow
     */
    public JobflowArtifact compile(Jobflow jobflow) throws IOException {
        BatchInfo batch = new BatchInfo.Basic(DummyBatchClass.ID, Descriptions.classOf(DummyBatchClass.class));
        return compile(batch, jobflow);
    }

    /**
     * Compiles the target jobflow.
     * @param batchId the container batch ID
     * @param jobflow the target jobflow
     * @return the compilation result
     * @throws IOException if failed to prepare the compilation environment
     * @throws DiagnosticException if failed to compile the target jobflow
     */
    public JobflowArtifact compile(String batchId, Jobflow jobflow) throws IOException {
        BatchInfo batch = new BatchInfo.Basic(batchId, Descriptions.classOf(DummyBatchClass.class));
        return compile(batch, jobflow);
    }

    /**
     * Compiles the target jobflow.
     * @param batch the structural information of a container batch
     * @param jobflow the target jobflow
     * @return the compilation result
     * @throws IOException if failed to prepare the compilation environment
     * @throws DiagnosticException if failed to compile the target jobflow
     */
    public JobflowArtifact compile(BatchInfo batch, Jobflow jobflow) throws IOException {
        Batch dummy = new Batch(batch);
        dummy.addElement(jobflow);
        BatchArtifact parent = compile(dummy);
        JobflowArtifact result = parent.findJobflow(jobflow.getFlowId());
        assert result != null;
        return result;
    }

    /**
     * Compiles the target batch.
     * @param batch the target batch
     * @return the compilation result
     * @throws IOException if failed to prepare the compilation environment
     * @throws DiagnosticException if failed to compile the target batch
     */
    public BatchArtifact compile(Batch batch) throws IOException {
        FileContainer output = createBatchOutput(batch);
        BatchCompiler.Context context = new BatchCompiler.Context(compilerContext, output);
        collector.reset();
        batchCompiler.compile(context, batch);
        BatchArtifact result = collector.take();
        return result;
    }

    /**
     * Copies the compiled artifact into the target batch applications directory.
     * @param artifact the target artifact
     * @param batchapps the target batch applications directory
     * @throws IOException if failed to copy the artifact
     */
    public void copyArtifact(JobflowArtifact artifact, File batchapps) throws IOException {
        copyArtifact(artifact.getBatch(), batchapps);
    }

    /**
     * Copies the compiled artifact into the target batch applications directory.
     * @param artifact the target artifact
     * @param batchapps the target batch applications directory
     * @throws IOException if failed to copy the artifact
     */
    public void copyArtifact(BatchArtifact artifact, File batchapps) throws IOException {
        copyArtifact(artifact.getReference(), batchapps);
    }

    private void copyArtifact(BatchInfo info, File batchapps) throws IOException {
        FileContainer source = computeBatchOutput(info, testerContext.getBatchApplicationHome());
        if (source.getBasePath().exists() == false) {
            throw new FileNotFoundException(source.getBasePath().getPath());
        }
        FileContainer destination = computeBatchOutput(info, batchapps);
        cleanOutput(destination);
        LOG.debug("copy: {} -> {}", source, destination); //$NON-NLS-1$
        try (ResourceSink sink = destination.createSink()) {
            ResourceUtil.copy(source, sink);
        }
    }

    private FileContainer createBatchOutput(BatchInfo batch) throws IOException {
        FileContainer container = computeBatchOutput(batch, testerContext.getBatchApplicationHome());
        cleanOutput(container);
        return container;
    }

    private FileContainer computeBatchOutput(BatchInfo batch, File batchapps) {
        File output = new File(batchapps, batch.getBatchId());
        return new FileContainer(output);
    }

    private void cleanOutput(FileContainer container) throws IOException {
        File output = container.getBasePath();
        if (output.exists()) {
            LOG.debug("cleaning output target: {}", container); //$NON-NLS-1$
            if (ResourceUtil.delete(output) == false) {
                throw new IOException(MessageFormat.format(
                        "failed to delete output target: {0}",
                        container));
            }
        }
    }

    @Override
    public void close() throws IOException {
        testerContext.removeTemporaryFiles();
        compilerContext.getProject().close();
    }

    private static final class ResultCollector extends AbstractCompilerParticipant {

        private BatchArtifact artifact;

        private final Map<String, ExternalPortMap> externalPorts = new LinkedHashMap<>();

        ResultCollector() {
            return;
        }

        void reset() {
            artifact = null;
            externalPorts.clear();
        }

        BatchArtifact take() {
            BatchArtifact result = artifact;
            reset();
            return result;
        }

        @Override
        public void beforeBatch(Context context, Batch batch) {
            externalPorts.clear();
        }

        @Override
        public void afterJobflow(JobflowCompiler.Context context, BatchInfo batch, Jobflow jobflow) {
            assert externalPorts.containsKey(jobflow.getFlowId()) == false;
            ExternalPortMap ports = ExternalPortMap.analyze(jobflow.getOperatorGraph(), context.getExternalPorts());
            externalPorts.put(jobflow.getFlowId(), ports);
        }

        @Override
        public void afterBatch(BatchCompiler.Context context, Batch batch, BatchReference reference) {
            assert artifact == null;
            List<JobflowArtifact> jobflows = new ArrayList<>();
            for (JobflowReference jobflow : reference.getJobflows()) {
                ExternalPortMap ports = externalPorts.get(jobflow.getFlowId());
                assert ports != null;
                jobflows.add(new JobflowArtifact(reference, jobflow, ports));
            }
            artifact = new BatchArtifact(reference, jobflows);
        }
    }
}
