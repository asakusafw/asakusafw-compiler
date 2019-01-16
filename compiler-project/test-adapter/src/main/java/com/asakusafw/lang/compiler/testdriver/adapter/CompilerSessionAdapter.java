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
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.analyzer.FlowGraphVerifier;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.ClassAnalyzer;
import com.asakusafw.lang.compiler.core.basic.BasicClassAnalyzer;
import com.asakusafw.lang.compiler.hadoop.HadoopCommandRequired;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.parameter.ImplicitParameterList;
import com.asakusafw.lang.compiler.tester.BatchArtifact;
import com.asakusafw.lang.compiler.tester.CompilerTester;
import com.asakusafw.lang.compiler.tester.ExternalPortMap;
import com.asakusafw.lang.compiler.tester.JobflowArtifact;
import com.asakusafw.testdriver.compiler.ArtifactMirror;
import com.asakusafw.testdriver.compiler.CompilerSession;
import com.asakusafw.testdriver.compiler.FlowPortMap;
import com.asakusafw.testdriver.compiler.basic.BasicArtifactMirror;
import com.asakusafw.testdriver.compiler.basic.BasicBatchMirror;
import com.asakusafw.testdriver.compiler.basic.BasicJobflowMirror;
import com.asakusafw.testdriver.compiler.basic.BasicPortMirror;
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
import com.asakusafw.workflow.model.CommandTaskInfo.ConfigurationResolver;
import com.asakusafw.workflow.model.CommandToken;
import com.asakusafw.workflow.model.DeleteTaskInfo;
import com.asakusafw.workflow.model.TaskInfo;
import com.asakusafw.workflow.model.attribute.ParameterInfo;
import com.asakusafw.workflow.model.attribute.ParameterListAttribute;
import com.asakusafw.workflow.model.basic.BasicCommandTaskInfo;
import com.asakusafw.workflow.model.basic.BasicDeleteTaskInfo;
import com.asakusafw.workflow.model.basic.BasicHadoopTaskInfo;
import com.asakusafw.workflow.model.basic.BasicTaskInfo;

class CompilerSessionAdapter implements CompilerSession {

    static final Logger LOG = LoggerFactory.getLogger(CompilerSessionAdapter.class);

    private static final Map<TaskReference.Phase, TaskInfo.Phase> PHASE_MAPPING;
    static {
        Map<TaskReference.Phase, TaskInfo.Phase> map = new EnumMap<>(TaskReference.Phase.class);
        for (TaskReference.Phase value : TaskReference.Phase.values()) {
            map.put(value, TaskInfo.Phase.valueOf(value.name()));
        }
        PHASE_MAPPING = map;
    }

    private static final ConfigurationResolver LAUNCHER_RESOLVER = new LauncherConfigurationResolver();

    private final CompilerConfigurationAdapter configuration;

    CompilerSessionAdapter(CompilerConfigurationAdapter configuration) {
        this.configuration = configuration;
    }

    @Override
    public ArtifactMirror compileBatch(Class<?> batchClass) throws IOException {
        try (CompilerTester tester = configuration.start(batchClass)) {
            Batch batch = tester.analyzeBatch(batchClass);
            BatchArtifact artifact = tester.compile(batch);
            return convert(artifact, batch, escape(tester, artifact.getReference()));
        } catch (DiagnosticException e) {
            throw translateException(e);
        }
    }

    @Override
    public ArtifactMirror compileJobflow(Class<?> jobflowClass) throws IOException {
        try (CompilerTester tester = configuration.start(jobflowClass)) {
            Jobflow jobflow = tester.analyzeJobflow(jobflowClass);
            JobflowArtifact artifact = tester.compile(jobflow);
            return convert(artifact, jobflow, escape(tester, artifact.getBatch()));
        } catch (DiagnosticException e) {
            throw translateException(e);
        }
    }

    @Override
    public ArtifactMirror compileFlow(FlowDescription flow, FlowPortMap portMap) throws IOException {
        if ((portMap instanceof FlowPortMapAdapter) == false) {
            throw new IllegalArgumentException();
        }
        try (CompilerTester tester = configuration.start(flow.getClass())) {
            FlowGraph graph = ((FlowPortMapAdapter) portMap).resolve(flow);
            FlowGraphVerifier.verify(graph);
            OperatorGraph analyzed = analyzeFlow(tester, flow, graph);
            JobflowInfo info = new JobflowInfo.Basic("flowpart", Descriptions.classOf(flow.getClass())); //$NON-NLS-1$
            Jobflow jobflow = new Jobflow(info, analyzed);
            JobflowArtifact artifact = tester.compile(jobflow);
            artifact.getReference().putAttribute(FlowGraph.class, graph);
            return convert(artifact, jobflow, escape(tester, artifact.getBatch()));
        } catch (DiagnosticException e) {
            throw translateException(e);
        }
    }

    private static OperatorGraph analyzeFlow(CompilerTester tester, FlowDescription flow, FlowGraph graph) {
        OperatorGraph results = new BasicClassAnalyzer()
                .analyzeFlow(new ClassAnalyzer.Context(tester.getCompilerContext()), graph);
        results.getInputs().values().stream()
                .filter(it -> it.isExternal() == false)
                .forEach(it -> {
                    throw new UnsupportedOperationException(MessageFormat.format(
                            "input \"{1}\" (in {0}) is unsupported (not managed by test driver)",
                            flow.getClass().getName(),
                            it.getName()));
                });
        results.getOutputs().values().stream()
                .filter(it -> it.isExternal() == false)
                .forEach(it -> {
                    throw new UnsupportedOperationException(MessageFormat.format(
                            "output \"{1}\" (in {0}) is unsupported (not managed by test driver)",
                            flow.getClass().getName(),
                            it.getName()));
                });
        return results;
    }

    private File escape(CompilerTester tester, BatchInfo batch) throws IOException {
        File batchapps = tester.getTesterContext().getBatchApplicationHome();
        File source = new File(batchapps, batch.getBatchId());
        File destination = new File(configuration.getWorkingDirectory(), UUID.randomUUID().toString());
        DeploymentUtil.deploy(source, destination, DeployOption.DELETE_SOURCE);
        return destination;
    }

    private ArtifactMirror convert(
            BatchArtifact artifact,
            Batch dsl,
            File outputDirectory) throws IOException {
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
        batch.addAttribute(convert(dsl, ImplicitParameterList.of(dsl)));
        return new BasicArtifactMirror(batch, outputDirectory);
    }

    private ArtifactMirror convert(
            JobflowArtifact artifact,
            Jobflow dsl,
            File outputDirectory) throws IOException {
        BasicJobflowMirror jobflow = toJobflowMirror(artifact);
        BasicBatchMirror batch = new BasicBatchMirror(artifact.getBatch().getBatchId());
        batch.addElement(jobflow);
        batch.addAttribute(new ParameterListAttribute(
                ImplicitParameterList.of(dsl).getParameters().stream()
                    .map(CompilerSessionAdapter::convert)
                    .collect(Collectors.toList()),
                false));
        return new BasicArtifactMirror(batch, outputDirectory);
    }

    private static ParameterListAttribute convert(BatchInfo info, ImplicitParameterList parameters) {
        return new ParameterListAttribute(
                parameters.merge(info.getParameters()).stream()
                        .map(CompilerSessionAdapter::convert)
                        .collect(Collectors.toList()),
                info.getAttributes().contains(BatchInfo.Attribute.STRICT_PARAMETERS));
    }

    private static ParameterInfo convert(BatchInfo.Parameter p) {
        return new ParameterInfo(
                p.getKey(),
                p.getComment(),
                p.isMandatory(),
                Optional.ofNullable(p.getPattern())
                    .map(Pattern::pattern)
                    .orElse(null));
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
                    BasicPortMirror<? extends ImporterDescription> mirror = new BasicPortMirror<>(
                            name,
                            info.getDataModelClass().resolve(classLoader),
                            info.getDescriptionClass().resolve(classLoader)
                                    .asSubclass(ImporterDescription.class)
                                    .newInstance());
                    result.addInput(mirror);
                } catch (ReflectiveOperationException e) {
                    throw new IOException(e);
                }
            }
        } else {
            for (FlowIn<?> port : graph.getFlowInputs()) {
                InputDescription desc = port.getDescription();
                BasicPortMirror<ImporterDescription> mirror = new BasicPortMirror<>(
                        port.getDescription().getName(),
                        (Class<?>) port.getDescription().getDataType(),
                        desc.getImporterDescription());
                result.addInput(mirror);
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
                    BasicPortMirror<? extends ExporterDescription> mirror = new BasicPortMirror<>(
                            name,
                            info.getDataModelClass().resolve(classLoader),
                            info.getDescriptionClass().resolve(classLoader)
                                    .asSubclass(ExporterDescription.class)
                                    .newInstance());
                    result.addOutput(mirror);
                } catch (ReflectiveOperationException e) {
                    throw new IOException(e);
                }
            }
        } else {
            for (FlowOut<?> port : graph.getFlowOutputs()) {
                OutputDescription desc = port.getDescription();
                BasicPortMirror<ExporterDescription> mirror = new BasicPortMirror<>(
                        port.getDescription().getName(),
                        (Class<?>) port.getDescription().getDataType(),
                        desc.getExporterDescription());
                result.addOutput(mirror);
            }
        }
    }

    private void processTasks(JobflowArtifact artifact, BasicJobflowMirror result) {
        for (Map.Entry<TaskReference.Phase, TaskInfo.Phase> mapping : PHASE_MAPPING.entrySet()) {
            Collection<? extends TaskReference> tasks = artifact.getReference().getTasks(mapping.getKey());
            Map<TaskReference, BasicTaskInfo> elements = new HashMap<>();
            for (TaskReference task : tasks) {
                BasicTaskInfo mirror = processTask(task);
                configuration.addTaskAttributes(artifact, task, mirror);
                result.addTask(mapping.getValue(), mirror);
                elements.put(task, mirror);
            }
            for (Map.Entry<TaskReference, BasicTaskInfo> entry : elements.entrySet()) {
                BasicTaskInfo blockee = entry.getValue();
                for (TaskReference blocker : entry.getKey().getBlockers()) {
                    blockee.addBlocker(elements.get(blocker));
                }
            }
        }
        if (isHadoopCommandRequired(artifact.getReference())) {
            result.addTask(TaskInfo.Phase.CLEANUP, new BasicDeleteTaskInfo(
                    DeleteTaskInfo.PathKind.HADOOP_FILE_SYSTEM,
                    Util.createStagePath()));
        }
    }

    private static boolean isHadoopCommandRequired(JobflowReference jobflow) {
        for (TaskReference.Phase phase : TaskReference.Phase.values()) {
            for (TaskReference task : jobflow.getTasks(phase)) {
                if (task instanceof HadoopTaskReference) {
                    return true;
                }
                if (HadoopCommandRequired.get(task)) {
                    return true;
                }
            }
        }
        return false;
    }

    private BasicTaskInfo processTask(TaskReference task) {
        if (task instanceof CommandTaskReference) {
            CommandTaskReference t = (CommandTaskReference) task;
            Location cmd = t.getCommand();
            List<CommandToken> args = resolveArguments(t.getArguments());
            BasicCommandTaskInfo.ConfigurationResolver resolver = findConfigurationResolver(cmd);
            return new BasicCommandTaskInfo(
                    t.getModuleName(), t.getProfileName(), cmd.toPath(), args, resolver);
        } else if (task instanceof HadoopTaskReference) {
            HadoopTaskReference t = (HadoopTaskReference) task;
            return new BasicHadoopTaskInfo(
                    t.getModuleName(), t.getMainClass().getBinaryName());
        } else {
            throw new UnsupportedOperationException(MessageFormat.format(
                    "unsupported task type: {0}",
                    task));
        }
    }

    private static List<CommandToken> resolveArguments(
            List<com.asakusafw.lang.compiler.api.reference.CommandToken> arguments) {
        List<CommandToken> results = new ArrayList<>();
        for (com.asakusafw.lang.compiler.api.reference.CommandToken token : arguments) {
            results.add(resolveCommandToken(token));
        }
        return results;
    }

    private static CommandToken resolveCommandToken(com.asakusafw.lang.compiler.api.reference.CommandToken token) {
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

    private BasicCommandTaskInfo.ConfigurationResolver findConfigurationResolver(Location path) {
        if (configuration.isAsakusaLauncher(path)) {
            return LAUNCHER_RESOLVER;
        } else {
            return null;
        }
    }

    private static IOException translateException(DiagnosticException exception) {
        for (Diagnostic diagnostic : exception.getDiagnostics()) {
            switch (diagnostic.getLevel()) {
            case ERROR:
                if (diagnostic.getException() == null) {
                    LOG.error(diagnostic.getMessage());
                } else {
                    LOG.error(diagnostic.getMessage(), diagnostic.getException());
                }
                break;
            case WARN:
                if (diagnostic.getException() == null) {
                    LOG.warn(diagnostic.getMessage());
                } else {
                    LOG.warn(diagnostic.getMessage(), diagnostic.getException());
                }
                break;
            case INFO:
                if (diagnostic.getException() == null) {
                    LOG.info(diagnostic.getMessage());
                } else {
                    LOG.info(diagnostic.getMessage(), diagnostic.getException());
                }
                break;
            default:
                throw new AssertionError(diagnostic);
            }
        }
        return new IOException(exception);
    }

    @Override
    public void close() throws IOException {
        return;
    }

    static final class LauncherConfigurationResolver implements ConfigurationResolver {

        @Override
        public List<CommandToken> apply(Map<String, String> configurations) {
            List<CommandToken> results = new ArrayList<>();
            for (Map.Entry<String, String> entry : configurations.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                results.add(CommandToken.of("--hadoop-conf")); //$NON-NLS-1$
                results.add(CommandToken.of(String.format("%s=%s", //$NON-NLS-1$
                        key, value)));
            }
            return results;
        }
    }
}
