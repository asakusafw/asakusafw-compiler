package com.asakusafw.lang.compiler.core.adapter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.basic.TaskContainer;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;

/**
 * An adapter for {@link JobflowProcessor}.
 */
public class JobflowProcessorAdapter implements JobflowProcessor.Context {

    private final JobflowCompiler.Context delegate;

    private final DataModelLoaderAdapter dataModelLoader;

    private final String batchId;

    private final String flowId;

    /**
     * Creates a new instance.
     * @param delegate the delegate target context
     * @param batch the structural information of the target batch
     * @param jobflow the structural information of the target jobflow
     */
    public JobflowProcessorAdapter(JobflowCompiler.Context delegate, BatchInfo batch, JobflowInfo jobflow) {
        this(delegate, batch.getBatchId(), jobflow.getFlowId());
    }

    /**
     * Creates a new instance.
     * @param delegate the delegate target context
     * @param batchId the target batch ID
     * @param flowId the target flow ID
     */
    public JobflowProcessorAdapter(JobflowCompiler.Context delegate, String batchId, String flowId) {
        this.delegate = delegate;
        this.batchId = batchId;
        this.flowId = flowId;
        this.dataModelLoader = new DataModelLoaderAdapter(delegate);
    }

    @Override
    public CompilerOptions getOptions() {
        return delegate.getOptions();
    }

    @Override
    public String getBatchId() {
        return batchId;
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getProject().getClassLoader();
    }

    @Override
    public DataModelLoader getDataModelLoader() {
        return dataModelLoader;
    }

    @Override
    public OutputStream addClassFile(ClassDescription aClass) throws IOException {
        return addResourceFile(Util.toClassFileLocation(aClass));
    }

    @Override
    public OutputStream addResourceFile(Location location) throws IOException {
        return delegate.getOutput().addResource(location);
    }

    @Override
    public ExternalInputReference addExternalInput(String name, ExternalInputInfo info) {
        ExternalPortProcessorAdapter adapter = newExternalPortProcessorAdapter();
        ExternalPortProcessor processor = delegate.getTools().getExternalPortProcessor();
        ExternalInputReference reference = processor.resolveInput(adapter, name, info);
        delegate.getExternalPorts().addInput(reference);
        return reference;
    }

    @Override
    public ExternalOutputReference addExternalOutput(
            String name,
            ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        ExternalPortProcessorAdapter adapter = newExternalPortProcessorAdapter();
        ExternalPortProcessor processor = delegate.getTools().getExternalPortProcessor();
        ExternalOutputReference reference = processor.resolveOutput(adapter, name, info, internalOutputPaths);
        delegate.getExternalPorts().addOutput(reference);
        return reference;
    }

    private ExternalPortProcessorAdapter newExternalPortProcessorAdapter() {
        return new ExternalPortProcessorAdapter(delegate, batchId, flowId);
    }

    @Override
    public TaskReference addTask(
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            TaskReference... blockers) {
        TaskContainer container = delegate.getTaskContainerMap().getMainTaskContainer();
        return addTask(container, moduleName, profileName, command, arguments, blockers);
    }

    @Override
    public TaskReference addFinalizer(
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            TaskReference... blockers) {
        TaskContainer container = delegate.getTaskContainerMap().getFinalizeTaskContainer();
        return addTask(container, moduleName, profileName, command, arguments, blockers);
    }

    private TaskReference addTask(
            TaskContainer container,
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            TaskReference... blockers) {
        CommandTaskReference reference =
                new CommandTaskReference(moduleName, profileName, command, arguments, Arrays.asList(blockers));
        container.add(reference);
        return reference;
    }

    @Override
    public <T> T getExtension(Class<T> extension) {
        return delegate.getExtension(extension);
    }
}
