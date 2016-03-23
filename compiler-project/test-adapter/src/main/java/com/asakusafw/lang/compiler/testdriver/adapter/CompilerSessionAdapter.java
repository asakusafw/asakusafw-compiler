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
package com.asakusafw.lang.compiler.testdriver.adapter;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.ClassAnalyzer;
import com.asakusafw.lang.compiler.core.basic.BasicClassAnalyzer;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.tester.BatchArtifact;
import com.asakusafw.lang.compiler.tester.CompilerTester;
import com.asakusafw.lang.compiler.tester.ExternalPortMap;
import com.asakusafw.lang.compiler.tester.JobflowArtifact;
import com.asakusafw.testdriver.compiler.ArtifactMirror;
import com.asakusafw.testdriver.compiler.CommandToken;
import com.asakusafw.testdriver.compiler.CompilerSession;
import com.asakusafw.testdriver.compiler.FlowPortMap;
import com.asakusafw.testdriver.compiler.TaskMirror;
import com.asakusafw.testdriver.compiler.basic.BasicArtifactMirror;
import com.asakusafw.testdriver.compiler.basic.BasicBatchMirror;
import com.asakusafw.testdriver.compiler.basic.BasicCommandTaskMirror;
import com.asakusafw.testdriver.compiler.basic.BasicHadoopTaskMirror;
import com.asakusafw.testdriver.compiler.basic.BasicJobflowMirror;
import com.asakusafw.testdriver.compiler.basic.BasicPortMirror;
import com.asakusafw.testdriver.compiler.basic.BasicTaskMirror;
import com.asakusafw.testdriver.compiler.util.DeploymentUtil;
import com.asakusafw.testdriver.compiler.util.DeploymentUtil.DeployOption;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;
import com.asakusafw.vocabulary.flow.graph.FlowIn;
import com.asakusafw.vocabulary.flow.graph.FlowOut;
import com.asakusafw.vocabulary.flow.graph.InputDescription;
import com.asakusafw.vocabulary.flow.graph.OutputDescription;

class CompilerSessionAdapter implements CompilerSession {

    private static final Map<TaskReference.Phase, TaskMirror.Phase> PHASE_MAPPING;
    static {
        Map<TaskReference.Phase, TaskMirror.Phase> map = new EnumMap<>(TaskReference.Phase.class);
        for (TaskReference.Phase value : TaskReference.Phase.values()) {
            map.put(value, TaskMirror.Phase.valueOf(value.name()));
        }
        PHASE_MAPPING = map;
    }

    private final CompilerConfigurationAdapter configuration;

    public CompilerSessionAdapter(CompilerConfigurationAdapter configuration) {
        this.configuration = configuration;
    }

    @Override
    public ArtifactMirror compileBatch(Class<?> batchClass) throws IOException {
        try (CompilerTester tester = configuration.start(batchClass)) {
            Batch batch = tester.analyzeBatch(batchClass);
            BatchArtifact artifact = tester.compile(batch);
            return convert(artifact, escape(tester, artifact.getReference()));
        } catch (DiagnosticException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ArtifactMirror compileJobflow(Class<?> jobflowClass) throws IOException {
        try (CompilerTester tester = configuration.start(jobflowClass)) {
            Jobflow jobflow = tester.analyzeJobflow(jobflowClass);
            JobflowArtifact artifact = tester.compile(jobflow);
            return convert(artifact, escape(tester, artifact.getBatch()));
        } catch (DiagnosticException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ArtifactMirror compileFlow(FlowDescription flow, FlowPortMap portMap) throws IOException {
        if ((portMap instanceof FlowPortMapAdapter) == false) {
            throw new IllegalArgumentException();
        }
        try (CompilerTester tester = configuration.start(flow.getClass())) {
            FlowGraph graph = ((FlowPortMapAdapter) portMap).resolve(flow);
            OperatorGraph analyzed = new BasicClassAnalyzer()
                    .analyzeFlow(new ClassAnalyzer.Context(tester.getCompilerContext()), graph);

            JobflowInfo info = new JobflowInfo.Basic("flowpart", Descriptions.classOf(flow.getClass())); //$NON-NLS-1$
            Jobflow jobflow = new Jobflow(info, analyzed);
            JobflowArtifact artifact = tester.compile(jobflow);
            artifact.getReference().putAttribute(FlowGraph.class, graph);
            return convert(artifact, escape(tester, artifact.getBatch()));
        } catch (DiagnosticException e) {
            throw new IOException(e);
        }
    }

    private File escape(CompilerTester tester, BatchInfo batch) throws IOException {
        File batchapps = tester.getTesterContext().getBatchApplicationHome();
        File source = new File(batchapps, batch.getBatchId());
        File destination = new File(configuration.getWorkingDirectory(), UUID.randomUUID().toString());
        DeploymentUtil.deploy(source, destination, DeployOption.DELETE_SOURCE);
        return destination;
    }

    private ArtifactMirror convert(BatchArtifact artifact, File outputDirectory) throws IOException {
        Map<JobflowReference, BasicJobflowMirror> elements = new LinkedHashMap<>();
        for (JobflowArtifact a : artifact.getJobflows()) {
            BasicJobflowMirror jobflow = toJobflowMirror(a);
            elements.put(a.getReference(), jobflow);
        }
        BasicBatchMirror batch = new BasicBatchMirror(artifact.getReference().getBatchId());
        for (Map.Entry<JobflowReference, BasicJobflowMirror> entry : elements.entrySet()) {
            BasicJobflowMirror jobflow = entry.getValue();
            batch.addElement(jobflow);
            for (JobflowReference blocker : entry.getKey().getBlockers()) {
                jobflow.addBlocker(elements.get(blocker));
            }
        }
        return new BasicArtifactMirror(batch, outputDirectory);
    }

    private ArtifactMirror convert(JobflowArtifact artifact, File outputDirectory) throws IOException {
        BasicJobflowMirror jobflow = toJobflowMirror(artifact);
        BasicBatchMirror batch = new BasicBatchMirror(artifact.getBatch().getBatchId());
        batch.addElement(jobflow);
        return new BasicArtifactMirror(batch, outputDirectory);
    }

    private BasicJobflowMirror toJobflowMirror(JobflowArtifact artifact) throws IOException {
        BasicJobflowMirror result = new BasicJobflowMirror(artifact.getReference().getFlowId());
        processInputs(artifact, result);
        processOutputs(artifact, result);
        processTasks(artifact, result);
        return result;
    }

    private void processInputs(JobflowArtifact artifact, BasicJobflowMirror result) throws IOException {
        FlowGraph graph = artifact.getReference().getAttribute(FlowGraph.class);
        if (graph == null) {
            ClassLoader classLoader = configuration.getClassLoader();
            ExternalPortMap ports = artifact.getExternalPorts();
            for (String name : ports.getInputs()) {
                ExternalInputInfo info = ports.findInputInfo(name);
                try {
                    result.addInput(new BasicPortMirror<>(
                            name,
                            info.getDataModelClass().resolve(classLoader),
                            info.getDescriptionClass().resolve(classLoader)
                                    .asSubclass(ImporterDescription.class)
                                    .newInstance()));
                } catch (ReflectiveOperationException e) {
                    throw new IOException(e);
                }
            }
        } else {
            for (FlowIn<?> port : graph.getFlowInputs()) {
                InputDescription desc = port.getDescription();
                result.addInput(new BasicPortMirror<>(
                        port.getDescription().getName(),
                        (Class<?>) port.getDescription().getDataType(),
                        desc.getImporterDescription()));
            }
        }
    }

    private void processOutputs(
            JobflowArtifact artifact, BasicJobflowMirror result) throws IOException {
        FlowGraph graph = artifact.getReference().getAttribute(FlowGraph.class);
        if (graph == null) {
            ClassLoader classLoader = configuration.getClassLoader();
            ExternalPortMap ports = artifact.getExternalPorts();
            for (String name : ports.getOutputs()) {
                ExternalOutputInfo info = ports.findOutputInfo(name);
                try {
                    result.addOutput(new BasicPortMirror<>(
                            name,
                            info.getDataModelClass().resolve(classLoader),
                            info.getDescriptionClass().resolve(classLoader)
                            .asSubclass(ExporterDescription.class)
                            .newInstance()));
                } catch (ReflectiveOperationException e) {
                    throw new IOException(e);
                }
            }
        } else {
            for (FlowOut<?> port : graph.getFlowOutputs()) {
                OutputDescription desc = port.getDescription();
                result.addOutput(new BasicPortMirror<>(
                        port.getDescription().getName(),
                        (Class<?>) port.getDescription().getDataType(),
                        desc.getExporterDescription()));
            }
        }
    }

    private void processTasks(JobflowArtifact artifact, BasicJobflowMirror result) {
        for (Map.Entry<TaskReference.Phase, TaskMirror.Phase> mapping : PHASE_MAPPING.entrySet()) {
            Collection<? extends TaskReference> tasks = artifact.getReference().getTasks(mapping.getKey());
            Map<TaskReference, BasicTaskMirror> elements = new HashMap<>();
            for (TaskReference task : tasks) {
                BasicTaskMirror mirror = processTask(task);
                result.addTask(mapping.getValue(), mirror);
                elements.put(task, mirror);
            }
            for (Map.Entry<TaskReference, BasicTaskMirror> entry : elements.entrySet()) {
                BasicTaskMirror blockee = entry.getValue();
                for (TaskReference blocker : entry.getKey().getBlockers()) {
                    blockee.addBlocker(elements.get(blocker));
                }
            }
        }
    }

    private BasicTaskMirror processTask(TaskReference task) {
        if (task instanceof CommandTaskReference) {
            CommandTaskReference t = (CommandTaskReference) task;
            List<CommandToken> args = resolveArguments(t.getArguments());
            return new BasicCommandTaskMirror(t.getModuleName(), t.getProfileName(), t.getCommand().toPath(), args);
        } else if (task instanceof HadoopTaskReference) {
            HadoopTaskReference t = (HadoopTaskReference) task;
            return new BasicHadoopTaskMirror(t.getModuleName(), t.getMainClass().getBinaryName());
        } else {
            throw new UnsupportedOperationException(MessageFormat.format(
                    "unsupported task type: {0}",
                    task));
        }
    }

    private List<CommandToken> resolveArguments(
            List<com.asakusafw.lang.compiler.api.reference.CommandToken> arguments) {
        List<CommandToken> results = new ArrayList<>();
        for (com.asakusafw.lang.compiler.api.reference.CommandToken token : arguments) {
            results.add(resolveCommandToken(token));
        }
        return results;
    }

    private CommandToken resolveCommandToken(com.asakusafw.lang.compiler.api.reference.CommandToken token) {
        switch (token.getTokenKind()) {
        case TEXT:
            return CommandToken.of(token.getImage());
        case BATCH_ID:
            return CommandToken.BATCH_ID;
        case FLOW_ID:
            return CommandToken.FLOW_ID;
        case BATCH_ARGUMENTS:
            return CommandToken.BATCH_ARGUMENTS;
        case EXECUTION_ID:
            return CommandToken.EXECUTION_ID;
        default:
            throw new AssertionError(token.getTokenKind());
        }
    }

    @Override
    public void close() throws IOException {
        return;
    }
}
